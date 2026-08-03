import easyocr

image_path = "D:/PythonProjects/portfolios/images/4.png"

def solve_captcha(image_path):
    reader = easyocr.Reader(["en"], gpu=False)
    result = reader.readtext(image_path, detail=0,
                             # allowlist="+0123456789"
                             )
    print(result)




import re
import cv2
import easyocr



image_path = "D:/PythonProjects/portfolios/images/2.png"

reader = easyocr.Reader(["en"], gpu=False)
result = reader.readtext(image_path, detail=0, allowlist="+0123456789")
print(result)



img = cv2.imread(image_path, cv2.IMREAD_GRAYSCALE)
reader = easyocr.Reader(["en"], gpu=False)
result = reader.readtext(img, detail=0,
                         # allowlist="+0123456789"
                         )
print(result)


gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
_, binary = cv2.threshold(gray, 130, 255, cv2.THRESH_BINARY)
cv2.imwrite(image_path.replace(f".{image_path.split(".")[-1]}", f"_.{image_path.split(".")[-1]}"), binary)
reader = easyocr.Reader(['en'], gpu=False)
result = reader.readtext(binary, detail=0)
print(result)


# cleaned_text = "".join(result).replace(" ", "")
# numbers = re.findall(r'\d+', cleaned_text)
# if len(numbers) == 2:
#     num1, num2 = map(int, numbers)