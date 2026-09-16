from captcha_solver.openocr_solver import captcha_solver as openocr_solver
from captcha_solver.paddleocr_solver import captcha_solver as paddleocr_solver


def solver(file_dir: str, solver_type: str):
    if solver_type == "paddleocr":
        result = paddleocr_solver(file_dir=file_dir)
    elif solver_type == "openocr":
        result = openocr_solver(file_dir=file_dir)
    else:
        raise ValueError(f"Unknown solver type: {solver_type}")
    return result


