def sleep(x):
    from time import sleep
    sleep(1)
    return 1

GB().map(sleep).run()