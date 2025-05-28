import scrapy
import re

class WeatherSpider(scrapy.Spider):
    name = "weather"
    allowed_domains = ["nchmf.gov.vn"]
    start_urls = ["https://nchmf.gov.vn/Kttv/vi-VN/1/thoi-tiet-bien-24h-s12h3-15.html"]
    # trích nguyên nội dung
    """def parse(self, response):
        # Tìm tất cả các khu vực trong danh sách
        rows = response.css("div.grp-list-item ul.uk-list li")
        for row in rows:
            khu_vuc = row.css("div.text-weather-location a::text").get()
            content = row.css("div.text-weather-location p::text").getall()
            mua = ""
            tam_nhin = ""
            gio = ""

           
            content = [line.strip() for line in content if line.strip()]

            
            if content:
                mua = content[0] if len(content) > 0 else ""
                tam_nhin = content[1] if len(content) > 1 else ""
                gio = content[2] if len(content) > 2 else ""

            yield {
                "Khu vực": khu_vuc,
                "Mưa": mua,
                "Tầm nhìn xa": tam_nhin,
                "Gió": gio,
            }"""
    # lọc lấy nội dung ngắn gọn
    def parse(self, response):
        # Tìm tất cả các khu vực trong danh sách
        rows = response.css("div.grp-list-item ul.uk-list li")
        for row in rows:
            khu_vuc = row.css("div.text-weather-location a::text").get()
            content = row.css("div.text-weather-location p::text").getall()

            content = [line.strip() for line in content if line.strip()]

            # Khởi tạo các giá trị mặc định
            mua = "Không"
            loc_xoay = "Không"
            gio_giat_manh = "Không"

            cap_gio_giat_manh = ""
            tam_nhin_xa = ""
            tam_nhin_xa_mua = ""
            huong_gio = ""
            cap_gio = ""
            do_cao_song = ""

            # Xử lý dòng 1: Mưa, Khả năng lốc xoáy, Cấp gió giật mạnh
            if content and len(content) > 0:
                line = content[0]
                # Mưa
                if "Có mưa" in line:
                    mua = "Có"
                # Khả năng lốc xoáy
                if "lốc xoáy" in line:
                    loc_xoay = "Có"
                # Cấp gió
                cap_gio_match = re.search(r"cấp (\d+(?:-\d+)?)", line)
                if cap_gio_match:
                    cap_gio_giat_manh = cap_gio_match.group(1)

            # Xử lý dòng 2: Tầm nhìn xa, Tầm nhìn xa trong mưa
            if content and len(content) > 1:
                line = content[1]
                # Tầm nhìn xa
                tam_nhin_match = re.search(r"Trên (\d+)km", line)
                if tam_nhin_match:
                    tam_nhin_xa = tam_nhin_match.group(1)
                # Tầm nhìn xa trong mưa
                tam_nhin_mua_match = re.search(r"giảm xuống (\d+(?:-\d+)?)km", line)
                if tam_nhin_mua_match:
                    tam_nhin_xa_mua = tam_nhin_mua_match.group(1)

            # Xử lý dòng 3: Hướng gió, Cấp gió, Độ cao sóng
            if content and len(content) > 2:
                line = content[2]
                # Hướng gió và Cấp gió
                if "Gió" in line:
                    gio_parts = line.split("cấp")
                    if len(gio_parts) > 1:
                        huong_gio = gio_parts[0].replace("Gió", "").strip()
                        cap_gio_match = re.search(r"cấp (\d+(?:-\d+)?)", line)
                        if cap_gio_match:
                            cap_gio = cap_gio_match.group(1)
                elif "Phía Tây" in line:
                    gio_parts = line.split("cấp")
                    if len(gio_parts) > 1:
                        huong_gio = gio_parts[0].strip()
                        cap_gio_match = re.search(r"cấp (\d+(?:-\d+)?)", line)
                        if cap_gio_match:
                            cap_gio = cap_gio_match.group(1)
                # Độ cao sóng
                song_match = re.search(r"Sóng cao (\d,\d\s*-\s*\d,\d)m", line)
                if song_match:
                    do_cao_song = song_match.group(1)

            yield {
                "Khu vực": khu_vuc,
                "Mưa": mua,
                "Khả năng lốc xoáy": loc_xoay,
                "Gió giật mạnh": gio_giat_manh,
                "Cấp gió trong giật mạnh": cap_gio_giat_manh,
                "Tầm nhìn xa": tam_nhin_xa,
                "Tầm nhìn xa trong mưa": tam_nhin_xa_mua,
                "Hướng gió": huong_gio,
                "Cấp gió": cap_gio,
                "Độ cao sóng": do_cao_song,
            }
            # gõ scrapy crawl weather -o data_thoitiet.csv để tải
        

