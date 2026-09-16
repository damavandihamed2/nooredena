from paddleocr import TextRecognition



def captcha_solver(file_dir: str):
    model = TextRecognition(
        model_name="PP-OCRv6_medium_rec",
        device="cpu",
    )
    file_dir = "captcha_solver/line.png"
    result = model.predict(input=file_dir, batch_size=1)
    print("Text:", result[0]["rec_text"])
    print("Confidence:", result[0]["rec_score"])

    return {
        "text": result[0]["rec_text"],
        "accuracy": result[0]["rec_score"]
    }
