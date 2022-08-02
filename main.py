from time import sleep, time
import threading


def calculate_sum_squares(n):
    sum_squares = 0
    for i in range(n):
        sum_squares += i ** 2
    print(sum_squares)


def sleep_a_little(sec):
    sleep(sec)


def main():
    calc_start_time = time()

    current_threads = []
    for i in range(5):
        max_value = (i + 1) * 1000000
        t = threading.Thread(target=calculate_sum_squares, args=(max_value,))
        t.start()
        current_threads.append(t)

    for i in range(len(current_threads)):
        current_threads[i].join()

    print(f"Calculation time: {round(time() - calc_start_time, 1)}")

    sleep_start_time = time()

    current_threads = []
    for i in range(1, 6):
        t = threading.Thread(target=sleep_a_little, args=(i,))
        t.start()
        t.join()
        # current_threads.append(t)

    # for i in range(len(current_threads)):
    #     current_threads[i].join()

    print(f"sleep took: {round(time() - sleep_start_time, 1)}")



if __name__ == "__main__":
    main()