import numpy as np
from openocr import OpenOCR




def captcha_solver(file_dir: str):
    model = OpenOCR(
        task="rec",
        mode="server",
        backend="torch",
        use_gpu="false",
    )

    allowed = set("0123456789+=")

    # دسترسی به بخش تبدیل خروجی شبکه به متن
    recognizer = model.model
    decoder = recognizer.post_process_class

    # بررسی اینکه مدل این کاراکترها را پشتیبانی می‌کند
    missing = allowed - set(decoder.character)
    if missing:
        raise ValueError(f"Unsupported characters: {sorted(missing)}")

    # توکن blank برای عملکرد درست CTC باید حفظ شود
    blank_ids = set(decoder.get_ignored_tokens())

    blocked = np.array([
        index not in blank_ids and char not in allowed
        for index, char in enumerate(decoder.character)
    ], dtype=bool)

    def limited_decode(preds, **kwargs):
        if kwargs.get("torch_tensor", True):
            preds = preds.detach().cpu().numpy()

        masked = preds.copy()
        masked[..., blocked] = -np.inf

        kwargs["torch_tensor"] = False
        return decoder(masked, **kwargs)

    recognizer.post_process_class = limited_decode

    result = model(image_path=file_dir)
    print("Text:", result[0]["text"])
    print("Confidence:", result[0]["score"])
    return {
        "text": result[0]["text"],
        "accuracy": result[0]["score"]
    }
