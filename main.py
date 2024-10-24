from flask import Flask, request, jsonify
from flask_restful import Api
from ultralytics import YOLO
from PIL import Image
import io
import json
import pypyodbc
import numpy as np

try:
    db = pypyodbc.connect(
        'DRIVER={SQL Server};'
        'SERVER=ESAD;'
        'DATABASE=kcetas;'
        'Trusted_Connection=True;'
        'Timeout=250;'
        'Connection Timeout=250;')
    imlec = db.cursor()
except pypyodbc.Error as e:
    print(f"Veritabanı bağlantı hatası: {e}")
    db, imlec = None, None

app = Flask(__name__)
api = Api(app)

model = YOLO('yeni.pt')


@app.route('/post', methods=['POST'])
def upload_file():
    if 'data' not in request.form:
        return jsonify({"error": "Bad Request: 'data' anahtarı eksik"}), 400

    data = request.form['data']
    try:
        data = json.loads(data)  # JSON string'i dict'e dönüştür
    except json.JSONDecodeError:
        successful = False
        return jsonify({"data": "", "message": "Invalid JSON format", "successful": successful}), 200

    IsEmriNo = data.get("IsEmriNo")
    IsEmriBirimi = data.get("IsEmriBirimi")

    if 'file' not in request.files:
        successful = False
        return jsonify({"data": "", "message": "No file part", "successful": successful}), 200

    file = request.files['file']

    # Dosya adı boş mu kontrol et
    if file.filename == '':
        successful = False
        return jsonify({"data": "", "message": "No sellected file", "successful": successful}), 200

    # Dosyayı aç ve işleme
    if file:
        try:
            image = Image.open(io.BytesIO(file.read()))
            image_array = np.array(image)

            # Model tahminlerini yap (model tanımlamanız ve eğitiminiz olması gerek)
            results = model.predict(source=image_array)

            # Veritabanına kayıt işlemi
            if not any(result.boxes for result in results):
                sql = ("INSERT INTO tablo1 (IstekZamanı, IsEmriNo, IsEmriBirimi, NesneAdı, GuvenSkoru) VALUES ("
                       "GETDATE(), ?, ?, 'Nesne algılanamadı', 0)")
                imlec.execute(sql, (IsEmriNo, IsEmriBirimi))
                db.commit()
            else:
                for result in results:
                    for detection in result.boxes:
                        conf = detection.conf.item()
                        cls = detection.cls.item()
                        print(f'Nesne: {model.names[int(cls)]}, Güven Skoru: {conf}')
                        sql = ("INSERT INTO tablo1 (IstekZamanı, IsEmriNo, IsEmriBirimi, NesneAdı, GuvenSkoru) VALUES ("
                               "GETDATE(), ?, ?, ?, ?)")
                        imlec.execute(sql, (IsEmriNo, IsEmriBirimi, model.names[int(cls)], conf))
                db.commit()

        except Exception as a:
            message = str(a)
            successful = False
            return jsonify({"data": "", "message": message, "successful": successful}), 200

    message = "hata yok"
    successful = True
    liste = get_records_by_latest_timestamp()
    return jsonify({"data": liste, "successful": successful, "message": message})


def get_records_by_latest_timestamp():
    try:
        # 1. Adım: En son saat ve dakika değerini bul
        sql_latest_timestamp = """
            SELECT MAX(CONVERT(VARCHAR, IstekZamanı, 120)) AS MaxDateTime
            FROM (
                SELECT DATEADD(MINUTE, DATEDIFF(MINUTE, 0, IstekZamanı), 0) AS IstekZamanı
                FROM tablo1
            ) AS T
        """
        imlec.execute(sql_latest_timestamp)
        latest_timestamp = imlec.fetchone()[0]

        # 2. Adım: Bu saat ve dakikaya sahip tüm kayıtları seç
        if latest_timestamp:
            sql_select_records = """
                SELECT id, IstekZamanı, IsEmriNo, IsEmriBirimi
                FROM tablo1 
                WHERE DATEADD(MINUTE, DATEDIFF(MINUTE, 0, IstekZamanı), 0) = ?
            """
            imlec.execute(sql_select_records, (latest_timestamp,))
            rows = imlec.fetchall()
            records = [{'id': row[0], 'IstekZamani': row[1], 'IsEmriNo': row[2], 'IsEmriBirimi': row[3], } for row in
                       rows]
            return records
        else:
            return {"message": "Kayıt bulunamadı"}
    except Exception as ete:
        return {"error": str(ete)}


def get_last_record():
    try:
        sql = "SELECT TOP 1 id, IstekZamanı, IsEmriNo, IsEmriBirimi FROM tablo1 ORDER BY id DESC"
        imlec.execute(sql)
        row = imlec.fetchone()
        if row:
            deger = {'id': row[0], 'IstekZamani': row[1], 'IsEmriNo': row[2], 'IsEmriBirimi': row[3]}
            return deger
        else:
            return {"message": "Kayıt bulunamadı"}
    except Exception as et:
        return {"error": str(et)}


# def getfiledb():
#     try:
#         sql = "SELECT id,IstekZamanı,IsEmriNo,IsEmriBirimi FROM tablo1"
#         imlec.execute(sql)
#         rows = imlec.fetchall()
#         deger = [{'id': row[0], 'IstekZamani': row[1], 'IsEmriNo': row[2], 'IsEmriBirimi': row[3]} for row in rows]
#         return deger
#
#     except Exception:
#         successful = False
#         message = "Veri tabanı bağlantısında sorun var"
#         print(message)
#         return jsonify({"data": "", "successful": successful, "message": message})
#
#
# @app.route('/get', methods=['GET'])
# def getfile():
#     try:
#         message = "hata yok"
#         successful = True
#         liste = getfiledb()
#         return jsonify({"data": liste, "successful": successful, "message": message})
#     except Exception:
#         successful = False
#         message = "Veri tabanı bağlantısında sorun var"
#         return jsonify({"data": "", "successful": successful, "message": message})
def getfiledb(filters):
    try:
        # Dinamik SQL sorgusu oluşturma
        base_sql = "SELECT id, IstekZamanı, IsEmriNo, IsEmriBirimi,NesneAdı,GuvenSkoru FROM tablo1 WHERE 1=1"
        params = []

        if filters.get('IsEmriNo'):
            base_sql += " AND IsEmriNo = ?"
            params.append(filters['IsEmriNo'])
        if filters.get('IsEmriBirimi'):
            base_sql += " AND IsEmriBirimi = ?"
            params.append(filters['IsEmriBirimi'])
        if filters.get('id'):
            base_sql += " AND id = ?"
            params.append(filters['id'])
        if filters.get('IstekZamani'):
            base_sql += " AND CONVERT(VARCHAR, IstekZamanı, 120) = ?"
            params.append(filters['IstekZamani'])


        imlec.execute(base_sql, params)
        rows = imlec.fetchall()
        deger = [{'id': row[0], 'IstekZamani': row[1], 'IsEmriNo': row[2], 'IsEmriBirimi': row[3], } for row in rows]
        return deger

    except Exception:
        successful = False
        message = "Veri tabanı bağlantısında sorun var ya da böyle bir kayıt yok"
        print(message)
        return jsonify({"data": "", "successful": successful, "message": message})


@app.route('/get', methods=['POST'])
def getfile():
    try:
        # JSON formatında veri al
        filters = request.json

        # Fonksiyonu çağır ve filtrelenmiş veriyi al
        liste = getfiledb(filters)

        message = "hata yok"
        successful = True
        return jsonify({"data": liste, "successful": successful, "message": message})

    except Exception:
        successful = False
        message = "Veri tabanı bağlantısında sorun var"
        return jsonify({"data": "", "successful": successful, "message": message})





@app.route('/delete', methods=['DELETE'])
def getdelete():
    sql1 = "TRUNCATE TABLE tablo1"
    sql2 = "DBCC CHECKIDENT (tablo1, RESEED, 1)"
    imlec.execute(sql1)
    imlec.execute(sql2)
    return "tablo başarı ile temizlendi", 200


if __name__ == '__main__':
    app.run(debug=True, port=8000)
