import logging
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel

logger = logging.getLogger("prepare_llm_chgnn_train_dataset")


def run_prepare_llm_chgnn_train_dataset( spark, interactions_path, item_nodes_path, output_path, negatives_per_query=20 ):
    """
    Tạo dataset train cho LLM-CHGNN:

    query Amazon item
    +
    positive VN item
    +
    negative VN candidates
    +
    text/spec/category

    Output:
        parquet train dataset
    """
    spark.conf.set("spark.sql.adaptive.enabled", "true")

    logger.info("Loading interactions...")
    df_inter = (spark.read.parquet(interactions_path)
                        .select( 
                            F.trim(F.col("asin")).alias("asin"),
                            F.trim(F.col("product_id")).alias("product_id")
                         )
                        .dropna()
                        .dropDuplicates(["asin", "product_id"]))

    logger.info("Loading item_nodes...")

    df_items = (spark.read.parquet(item_nodes_path)
                        .select( "product_id", "asin", "full_text", "parsed_specs", "category", "domain")
                        .filter(F.col("full_text").isNotNull())
                        .filter(F.col("full_text") != ""))

    # =========================
    # AMAZON QUERY ITEMS
    # =========================

    df_amz = ( df_items.filter(F.col("domain") == "amazon")
                        .select(
                            F.trim(F.col("asin")).alias("query_asin"),
                            F.col("full_text").alias("query_text"),
                            F.col("parsed_specs").alias("query_specs"),
                            F.col("category").alias("query_category"),
                        )
              )
    
    # =========================
    # VN POSITIVE ITEMS
    # =========================

    df_vn = (
        df_items
        .filter(F.col("domain") == "vn")
        .select(
            F.trim(F.col("product_id")).alias("vn_product_id"),
            F.col("full_text").alias("candidate_text"),
            F.col("parsed_specs").alias("candidate_specs"),
            F.col("category").alias("candidate_category"),
        )
        .filter(F.col("vn_product_id").isNotNull())
        .filter(F.col("vn_product_id") != "")
        # .dropDuplicates(["vn_product_id"])
    )

    logger.info("Building exact positive pairs...")

    df_pos_exact = (
        df_inter
        .join(df_amz, df_inter["asin"] == df_amz["query_asin"], "inner")
        .join(df_vn, df_inter["product_id"] == df_vn["vn_product_id"], "inner")
        .select(
            "query_asin", "query_text", "query_specs", "query_category",
            F.col("vn_product_id").alias("positive_product_id"),
            F.col("candidate_text").alias("positive_text"),
            F.col("candidate_specs").alias("positive_specs"),
            F.col("candidate_category").alias("positive_category")
        )
        # .dropDuplicates(["query_asin", "positive_product_id"])
        .withColumn("source_type", F.lit("exact"))
    )

    logger.info("Building pseudo positive pairs from same-category Amazon-VN items...")

    pseudo_amz_per_category = 50
    pseudo_per_vn = 5

    w_amz_cat = Window.partitionBy("query_category").orderBy(F.rand(seed=123))

    df_amz_pool = (
        df_amz
        .filter(F.col("query_category").isNotNull())
        .filter(F.col("query_category") != "")
        .filter(F.col("query_category") != "other")
        .withColumn("amz_rank", F.row_number().over(w_amz_cat))
        .filter(F.col("amz_rank") <= pseudo_amz_per_category)
        .drop("amz_rank")
    )

    df_vn_pos = (
        df_vn
        .filter(F.col("candidate_category").isNotNull())
        .filter(F.col("candidate_category") != "")
        .filter(F.col("candidate_category") != "other")
    )

    df_pseudo_raw = (
        df_vn_pos
        .join(
            F.broadcast(df_amz_pool),
            df_vn_pos["candidate_category"] == df_amz_pool["query_category"],
            "inner"
        )
        .select(
            "query_asin",
            "query_text",
            "query_specs",
            "query_category",
            F.col("vn_product_id").alias("positive_product_id"),
            F.col("candidate_text").alias("positive_text"),
            F.col("candidate_specs").alias("positive_specs"),
            F.col("candidate_category").alias("positive_category")
        )
    )

    w_pseudo = Window.partitionBy("positive_product_id").orderBy(F.rand(seed=456))

    df_pos_pseudo = (
        df_pseudo_raw
        .withColumn("pseudo_rank", F.row_number().over(w_pseudo))
        .filter(F.col("pseudo_rank") <= pseudo_per_vn)
        .drop("pseudo_rank")
        .withColumn("source_type", F.lit("pseudo_same_category"))
    )

    df_pos = (
        df_pos_exact
        .unionByName(df_pos_pseudo)
        .dropDuplicates(["query_asin", "positive_product_id"])
    )
    
    # Tạo key đơn giản để groupBy, tránh groupBy MAP
    df_pos = df_pos.withColumn( "pair_id", F.sha2(F.concat_ws("||", F.col("query_asin"), F.col("positive_product_id")), 256) )

    df_pos_keys = df_pos.select( "pair_id", "query_asin", "positive_product_id", "query_category")

    logger.info("Preparing bounded negative pools...")

    # Hard pool: tối đa 50 item/category
    w_cat = Window.partitionBy("candidate_category").orderBy(F.rand(seed=42))
    df_vn_hard_pool = (
        df_vn
        .withColumn("rn", F.row_number().over(w_cat))
        .filter(F.col("rn") <= 50)      # Hard pool: tối đa 50 item/category
        .drop("rn")
    )

    # Easy pool: random global pool giới hạn, giảm từ 5000 về 2000 cho limit thôi
    df_vn_easy_pool = (
        df_vn
        .orderBy(F.rand(seed=42))
        .limit(500)
    )

    logger.info("Generating hard negatives...")
    df_neg_hard = (
        df_pos_keys
        .join(
            F.broadcast(df_vn_hard_pool),
            (df_pos_keys["query_category"] == df_vn_hard_pool["candidate_category"]) &
            (df_pos_keys["positive_product_id"] != df_vn_hard_pool["vn_product_id"]) &
            (df_pos_keys["query_category"] != "other"),
            "inner",
        )
        .select(
            "pair_id",
            F.col("vn_product_id").alias("neg_id"),
            F.col("candidate_text").alias("neg_text"),
            F.col("candidate_specs").alias("neg_specs"),
            F.col("candidate_category").alias("neg_category"),
        )
        .withColumn("priority", F.lit(1))
    )

    logger.info("Generating easy negatives...")
    df_neg_easy = (
        df_pos_keys
        .join(
            F.broadcast(df_vn_easy_pool),
            df_pos_keys["positive_product_id"] != df_vn_easy_pool["vn_product_id"],
            "inner",
        )
        .select(
            "pair_id",
            F.col("vn_product_id").alias("neg_id"),
            F.col("candidate_text").alias("neg_text"),
            F.col("candidate_specs").alias("neg_specs"),
            F.col("candidate_category").alias("neg_category"),
        )
        .withColumn("priority", F.lit(0))
    )

    logger.info("Ranking negatives...")
    df_neg_all = df_neg_hard.unionByName(df_neg_easy)

    w_neg = Window.partitionBy("pair_id").orderBy(
        F.col("priority").desc(),
        F.rand(seed=42)
    )

    df_neg_topk = (
        df_neg_all
        # .dropDuplicates(["pair_id", "neg_id"])
        .withColumn("neg_rank", F.row_number().over(w_neg))
        .filter(F.col("neg_rank") <= negatives_per_query)
    )

    logger.info("Aggregating negatives by pair_id...")
    df_neg_grouped = (
        df_neg_topk
        .groupBy("pair_id")
        .agg(
            F.collect_list("neg_id").alias("negative_ids"),
            F.collect_list("neg_text").alias("negative_texts"),
            F.collect_list("neg_specs").alias("negative_specs"),
            F.collect_list("neg_category").alias("negative_categories"),
        )
    )

    logger.info("Joining positives back and formatting final dataset...")
    df_final = (
        df_pos
        .join(df_neg_grouped, "pair_id", "inner")
        .withColumn(
            "candidate_ids",
            F.concat(F.array(F.col("positive_product_id")), F.col("negative_ids"))
        )
        .withColumn(
            "candidate_texts",
            F.concat(F.array(F.col("positive_text")), F.col("negative_texts"))
        )
        .withColumn(
            "candidate_specs",
            F.concat(F.array(F.col("positive_specs")), F.col("negative_specs"))
        )
        .withColumn(
            "candidate_categories",
            F.concat(F.array(F.col("positive_category")), F.col("negative_categories"))
        )
        .withColumn("true_vn_id", F.col("positive_product_id"))
        .select(
            "query_asin",
            "query_text",
            "query_specs",
            "query_category",
            "candidate_ids",
            "candidate_texts",
            "candidate_specs",
            "candidate_categories",
            "true_vn_id",
        )
    )

    logger.info(f"Writing LLM-CHGNN train dataset -> {output_path}")
    df_final.coalesce(8).write.mode("overwrite").parquet(output_path)

    final_count = spark.read.parquet(output_path).count()
    logger.info(f"LLM-CHGNN train dataset done: {final_count:,}")
    return final_count