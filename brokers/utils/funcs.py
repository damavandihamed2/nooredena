from time import sleep

def safe_call_func(func: object, sleep_time: int = 1, **kwargs):
    counter = 1
    while True:
        print(f"Try {counter} for {func.__name__}")
        try:
            func(**kwargs)
            break
        except Exception as e:
            print(e)
            print(f"---------- Waiting {sleep_time} seconds ----------")
            sleep(sleep_time)
        counter += 1


def safe_login(broker_class: callable, sleep_time: int = 2, **kwargs):
    counter = 1
    while True:
        print(f"Try {counter} on login")
        try:
            broker = broker_class(**kwargs)
            broker.login()
            if broker.loginResponseStatus == 200:
                break
            else:
                print(f"Waiting for {sleep_time} seconds")
                sleep(sleep_time)
        except Exception as e:
            print(e)
            print(f"Waiting for {sleep_time} seconds")
            sleep(sleep_time)
        counter += 1
    return broker
