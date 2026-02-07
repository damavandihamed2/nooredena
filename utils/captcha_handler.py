import io, base64
import PIL.Image as pil

from utils.database import make_connection


db_conn = make_connection()


def update_captcha_value(captcha_type: str, captcha_id: str, captcha_value: str) -> None:
    crsr = db_conn.cursor()
    crsr.execute(f"UPDATE [nooredenadb].[extra].[captcha_images] SET "
                 f"captcha_value='{captcha_value}' "
                 f"WHERE captcha_id='{captcha_id}' AND captcha_type='{captcha_type}'")
    crsr.close()


def save_captcha(captcha_type: str, captcha_image, captcha_id: str, b64decode: bool = True) -> None:

    if b64decode:
        image = base64.b64decode(captcha_image)
    else:
        image = captcha_image

    crsr = db_conn.cursor()
    crsr.execute(f"INSERT INTO [nooredenadb].[extra].[captcha_images] (captcha_type, captcha_image, "
                 f"captcha_value, captcha_id) VALUES (?, ?, ?, ?)",
                 (f'{captcha_type}', image, None, captcha_id))
    crsr.close()



def open_captcha(captcha_type: str, captcha_id: str) -> str:
    crsr = db_conn.cursor()
    crsr.execute(f"SELECT captcha_image FROM [nooredenadb].[extra].[captcha_images] "
                 f"WHERE captcha_id='{captcha_id}' AND captcha_type='{captcha_type}'")
    img_ = crsr.fetchall()[0][0]
    crsr.close()
    img = pil.open(io.BytesIO(img_))
    img.resize(size=(img.size[0] * 3, img.size[1] * 3)).show()
    captcha_value = input("Please Enter The Captcha Phrase: ")
    img.close()
    return captcha_value

