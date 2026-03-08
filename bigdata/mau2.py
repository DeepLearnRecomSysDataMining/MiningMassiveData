import streamlit as st
import requests

# Tiêu đề ứng dụng
st.title("Hệ Thống Tìm Kiếm & Gợi Ý Sản Phẩm Amazon")

# Ô nhập từ khóa (Hỗ trợ tiếng Việt)
query = st.text_input("Nhập sản phẩm bạn muốn tìm (VD: Loa bass mạnh đi phượt):")

if st.button("Tìm kiếm"):
    if query:
        with st.spinner("Đang tìm kiếm..."):
            # Gọi API Search từ Thành viên 2 (Ví dụ URL API local)
            # api_url = "http://localhost:8000/search"
            # response = requests.post(api_url, json={"query": query})
            
            # Giả lập kết quả trả về
            st.success("Đã tìm thấy các sản phẩm phù hợp!")
            st.write("1. Loa Bluetooth Sony Extra Bass...")
            st.write("2. Loa JBL Charge 5...")
            
            # --- Phần Gợi Ý (Sẽ gọi API Recommendation ở đây) ---
            st.markdown("---")
            st.subheader("Sản phẩm tương tự (Có thể bạn sẽ thích)")
            st.write("- Loa Marshall Emberton (Cùng cụm tính năng)")
            st.write("- Tai nghe Sony Extra Bass (Cùng sở thích âm bass)")
    else:
        st.warning("Vui lòng nhập từ khóa tìm kiếm!")