import os
import glob
import json

input_folder = "reviews"
output_file = "reviews_amazon.jsonl"

jsonl_files = glob.glob(os.path.join(input_folder, "*.jsonl"))

print(f"Found {len(jsonl_files)} files\n")

total_valid = 0
total_invalid = 0

with open(output_file, "w", encoding="utf-8") as outfile:
    for file in jsonl_files:
        valid_count = 0
        invalid_count = 0

        print(f"Processing: {file}")

        with open(file, "r", encoding="utf-8") as infile:
            for line in infile:
                line = line.strip()

                # Bỏ dòng rỗng
                if not line:
                    continue

                # Bỏ dòng conflict git
                if any(x in line for x in ["<<<<<<<", "=======", ">>>>>>>", "HEAD"]):
                    invalid_count += 1
                    continue

                # Kiểm tra JSON hợp lệ
                try:
                    obj = json.loads(line)
                    outfile.write(json.dumps(obj, ensure_ascii=False) + "\n")
                    valid_count += 1
                except json.JSONDecodeError:
                    invalid_count += 1
                    continue

        print(f"  ✅ Valid JSON lines: {valid_count}")
        print(f"  ❌ Invalid lines: {invalid_count}\n")

        total_valid += valid_count
        total_invalid += invalid_count

print("===== SUMMARY =====")
print(f"Total valid: {total_valid}")
print(f"Total invalid: {total_invalid}")
print(f"Output saved to: {output_file}")