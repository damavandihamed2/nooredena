import io
import cv2
# import easyocr
import warnings
import pytesseract
import numpy as np
import pandas as pd
from tqdm import tqdm
import PIL.Image as pil


from utils.database import make_connection


warnings.filterwarnings("ignore")
db_conn = make_connection()
file_dir = "./utils/tmp_.png"
# reader = easyocr.Reader(["en"], gpu=False)

query_rayan = """
SELECT captcha_image, captcha_value FROM [nooredenadb].[extra].[captcha_images]
WHERE captcha_value IS NOT NULL
    AND captcha_id > '1784975000'
    AND captcha_type = 'rayanhamafza'
--    AND captcha_type = 'tadbirpardaz'
"""

query_tadbir = """
SELECT captcha_image, captcha_value FROM [nooredenadb].[extra].[captcha_images]
WHERE captcha_value IS NOT NULL
    AND captcha_type = 'tadbirpardaz'
"""


captcha_images = pd.read_sql(query_tadbir, db_conn)



# def solve_with_easyocr(file_path, image_bytes):
#     file_solver = bytes_solver = ensemble_solver = ""
#     result_bytes = reader.readtext(image_bytes, allowlist="0123456789")
#     result_file = reader.readtext(file_path, allowlist="0123456789")
#     if (not result_bytes) and (not result_file):
#         pass
#     elif not result_bytes:
#         file_solver = ensemble_solver = result_file[0][-2]
#     elif not result_file:
#         bytes_solver = ensemble_solver = result_bytes[0][-2]
#     else:
#         file_solver, bytes_solver = result_file[0][-2], result_bytes[0][-2]
#         result_file_prob, result_bytes_prob = result_file[0][-1], result_bytes[0][-1]
#         if result_file_prob >= result_bytes_prob:
#             ensemble_solver = file_solver
#         else:
#             ensemble_solver = bytes_solver
#     return {
#     "easyocr_file_solver": file_solver,
#     "easyocr_bytes_solver": bytes_solver,
#     "easyocr_ensemble_solver": ensemble_solver
#     }


def preprocess_for_ocr(image_path):
    img = cv2.imread(image_path)
    # 1) grayscale
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    # 2) denoise
    gray = cv2.bilateralFilter(gray, 9, 75, 75)
    # 3) enlarge
    scale = 2
    gray = cv2.resize(gray, None, fx=scale, fy=scale, interpolation=cv2.INTER_CUBIC)
    # 4) threshold
    _, th = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    # 5) morphology to clean small noise
    kernel = np.ones((2, 2), np.uint8)
    th = cv2.morphologyEx(th, cv2.MORPH_OPEN, kernel)
    return th


tesseract_config = ("--oem 1"
                    " --psm 7"
                    " -c tessedit_char_whitelist=0123456789"
                    # " -c load_system_dawg=0"
                    # " -c load_freq_dawg=0"
                    # " -c tessedit_do_invert=1"
                    )


def solve_with_tesseract(file_path):
    result_tesseract = pytesseract.image_to_string(pil.open(file_path), lang="eng", config=tesseract_config).strip()
    result_tesseract_processed = pytesseract.image_to_string(preprocess_for_ocr(file_path), lang="eng", config=tesseract_config).strip()

    result_tesseract_standard = pytesseract.image_to_string(pil.open(file_path), lang="eng_standard", config=tesseract_config).strip()
    result_tesseract_processed_standard = pytesseract.image_to_string(preprocess_for_ocr(file_path), lang="eng_standard", config=tesseract_config).strip()

    result_tesseract_best = pytesseract.image_to_string(pil.open(file_path), lang="eng_best", config=tesseract_config).strip()
    result_tesseract_processed_best = pytesseract.image_to_string(preprocess_for_ocr(file_path), lang="eng_best", config=tesseract_config).strip()
    return {
        "tesseract_solver": result_tesseract, "tesseract_processed_solver": result_tesseract_processed,
        "solve_tesseract_standard": result_tesseract_standard, "solve_tesseract_processed_standard": result_tesseract_processed_standard,
        "solve_tesseract_best": result_tesseract_best, "solve_tesseract_processed_best": result_tesseract_processed_best
    }


captcha_images[[
    # "results_easyocr_file", "results_easyocr_bytes", "results_easyocr_ensemble",
    "solve_tesseract", "solve_tesseract_processed",
    "solve_tesseract_standard", "solve_tesseract_processed_standard",
    "solve_tesseract_best", "solve_tesseract_processed_best"

]] = (
    # None, None, None,
    None, None,
    None, None,
    None, None
)

for i in tqdm(range(len(captcha_images))):
    image = captcha_images["captcha_image"].iloc[i]
    img = pil.open(io.BytesIO(image))
    img.save(file_dir)

    # results_easyocr = solve_with_easyocr(file_path=file_dir, image_bytes=image)
    # results_easyocr_file = results_easyocr["easyocr_file_solver"]
    # results_easyocr_bytes = results_easyocr["easyocr_bytes_solver"]
    # results_easyocr_ensemble = results_easyocr["easyocr_ensemble_solver"]

    result_tesseract = solve_with_tesseract(file_dir)

    result_tesseract_file = result_tesseract["tesseract_solver"]
    result_tesseract_processed = result_tesseract["tesseract_processed_solver"]

    result_tesseract_standard = result_tesseract["solve_tesseract_standard"]
    result_tesseract_processed_standard = result_tesseract["solve_tesseract_processed_standard"]

    result_tesseract_best = result_tesseract["solve_tesseract_best"]
    result_tesseract_processed_best = result_tesseract["solve_tesseract_processed_best"]



    captcha_images.loc[
        i, [
            # "results_easyocr_file", "results_easyocr_bytes", "results_easyocr_ensemble",
            "solve_tesseract", "solve_tesseract_processed",
            "solve_tesseract_standard", "solve_tesseract_processed_standard",
            "solve_tesseract_best", "solve_tesseract_processed_best"
        ]
    ] = (
        # results_easyocr_file, results_easyocr_bytes,results_easyocr_ensemble,
        result_tesseract_file, result_tesseract_processed,
        result_tesseract_standard, result_tesseract_processed_standard,
        result_tesseract_best, result_tesseract_processed_best
    )





results_accuracy = pd.DataFrame()
for c in [
    # "results_easyocr_file", "results_easyocr_bytes", "results_easyocr_ensemble",
    "solve_tesseract", "solve_tesseract_processed",
    "solve_tesseract_standard", "solve_tesseract_processed_standard",
    "solve_tesseract_best", "solve_tesseract_processed_best"
]:
    acc = (captcha_images[c] == captcha_images["captcha_value"]).sum() / len(captcha_images)
    tmp = pd.DataFrame([{"solve_type": c, "accuracy": acc}])
    results_accuracy = pd.concat([results_accuracy, tmp], axis=0, ignore_index=True)



####################################################################################################
####################################################################################################
####################################################################################################

"""
Tadbir

tesseract_config = ("--oem 1 "
                    "--psm 8 "
                    "-c tessedit_char_whitelist=0123456789 ")
solve_tesseract,0.43061516452074394
solve_tesseract_processed,0.2932761087267525
solve_tesseract_standard,0.5779685264663805
solve_tesseract_processed_standard,0.3719599427753934
solve_tesseract_best,0.5722460658082976
solve_tesseract_processed_best,0.3834048640915594


tesseract_config = ("--oem 1 "
                    "--psm 7 "
                    "-c tessedit_char_whitelist=0123456789 ")
solve_tesseract,0.44778254649499283
solve_tesseract_processed,0.2675250357653791
solve_tesseract_standard,0.5464949928469242
solve_tesseract_processed_standard,0.33905579399141633
solve_tesseract_best,0.5464949928469242
solve_tesseract_processed_best,0.33905579399141633

"""

####################################################################################################
####################################################################################################
####################################################################################################

"""
Rayan

tesseract_config = ("--oem 1"
                    " --psm 8"
                    " -c tessedit_char_whitelist=0123456789")
solve_tesseract,0.1711229946524064
solve_tesseract_processed,0.15508021390374332
solve_tesseract_standard,0.26737967914438504
solve_tesseract_processed_standard,0.20855614973262032
solve_tesseract_best,0.18181818181818182
solve_tesseract_processed_best,0.22459893048128343


tesseract_config = ("--oem 1"
                    " --psm 7"
                    " -c tessedit_char_whitelist=0123456789")
solve_tesseract,0.08021390374331551
solve_tesseract_processed,0.20855614973262032
solve_tesseract_standard,0.20320855614973263
solve_tesseract_processed_standard,0.24598930481283424
solve_tesseract_best,0.1657754010695187
solve_tesseract_processed_best,0.25668449197860965


"""

####################################################################################################
####################################################################################################
####################################################################################################
