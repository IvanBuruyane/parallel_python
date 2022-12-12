import time
from multiprocessing import Pool, cpu_count


def check_number_of_values_in_range(comp_list, lower, upper):
    number_of_hits = 0
    for i in range(lower, upper):
        for number in comp_list:
            if not i % number:
                number_of_hits += 1
                break
    return number_of_hits


if __name__ == '__main__':
    start_time = time.time()
    num_processes = 4
    comparison_list = [12, 48, 167]
    lower_and_upper_bounds = (0, 10 ** 8)

    num_cpu_to_use = max(1, cpu_count() - 1)
    print('Number of cpus being used:', num_cpu_to_use)

    prepared_list = [(comparison_list, *lower_and_upper_bounds)]

    print('List to use as input:', prepared_list)
    with Pool(num_cpu_to_use) as mp_pool:
        result = mp_pool.starmap(check_number_of_values_in_range, prepared_list,
                                 chunksize=int(lower_and_upper_bounds[1] / num_cpu_to_use))

    # One process implementation (which is faster)
    # result = check_number_of_values_in_range(comparison_list, *lower_and_upper_bounds)

    print(result)
    print("Time:" + str(time.time() - start_time))