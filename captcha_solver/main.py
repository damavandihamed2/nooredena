import io
import warnings
import numpy as np
import pandas as pd

from utils.database import make_connection
from captcha_solver.solver import solver

from tqdm import tqdm
import PIL.Image as pil



warnings.filterwarnings("ignore")
db_conn = make_connection()


captcha_df = pd.read_sql("SELECT * FROM [nooredenadb].[extra].[captcha_images] WHERE captcha_type = 'omex' and captcha_value is not null", db_conn)
captcha_df["paddle_result"] = ""
captcha_df["paddle_accuracy"] = np.nan
captcha_df["open_result"] = ""
captcha_df["open_accuracy"] = np.nan


def save_captcha(image, file_dir: str = "tmp.jpeg"):
    img = pil.open(io.BytesIO(image))
    img.save(file_dir)



for i in tqdm(range(len(captcha_df))):
    image = captcha_df["captcha_image"].iloc[i]
    save_captcha(image)
    result_paddle = solver(file_dir="tmp.jpeg", solver_type="paddleocr")
    result_open = solver(file_dir="tmp.jpeg", solver_type="openocr")

    captcha_df["paddle_result"].iloc[i] = result_paddle["text"]
    captcha_df["paddle_accuracy"].iloc[i] = result_paddle["accuracy"]

    captcha_df["open_result"].iloc[i] = result_open["text"]
    captcha_df["open_accuracy"].iloc[i] = result_open["accuracy"]


def add_numbers(text: str) -> int | None:
    result = text.split("=")[0]
    result = "".join([result[j] for j in range(len(result)) if result[j] in "1234567890+"])
    if (len(result.split("+")) == 2) and (result.split("+")[0] != "") and (result.split("+")[1] != ""):
        return int(result.split("+")[0]) + int(result.split("+")[1])


captcha_df["paddle_result_final"] = None
captcha_df["open_result_final"] = None

for i in tqdm(range(len(captcha_df))):
    result = captcha_df["paddle_result"].iloc[i]
    result = add_numbers(result)
    if result:
        captcha_df["paddle_result_final"].iloc[i] = result

    result_ = captcha_df["open_result"].iloc[i]
    result_ = add_numbers(result_)
    if result_:
        captcha_df["open_result_final"].iloc[i] = result_


captcha_df["paddle"] = captcha_df["paddle_result_final"] == captcha_df["captcha_value"].astype(int)
captcha_df["open"] = captcha_df["open_result_final"] == captcha_df["captcha_value"].astype(int)


(captcha_df["paddle_result_final"] == captcha_df["captcha_value"].astype(int)).sum() / len(captcha_df)
(captcha_df["open_result_final"] == captcha_df["captcha_value"].astype(int)).sum() / len(captcha_df)

captcha_df.drop(columns=["captcha_image"], inplace=False).to_excel("c:/users/h.damavandi/desktop/captcha_df.xlsx", index=False)



