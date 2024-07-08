from database.filename_generator import generate_unique_name

from database.db_operations import insert_file_info


def user_input():
    orders = []
    files = []
    print("Enter size, unit (b, kb, mb, gb, tb) and count (e.g., '17 mb 100') or 'end': ")
    while True:
        response = input()
        if response.strip().lower() == 'end':
            try:
                for order in orders:
                    size, unit, count = order.split()
                    if unit == "b":
                        size = int(size)
                    elif unit == "kb":
                        size = int(size) * 1024
                    elif unit == "mb":
                        size = int(size) * 1024 * 1024
                    elif unit == "gb":
                        size = int(size) * 1024 * 1024 * 1024
                    elif unit == "tb":
                        size = int(size) * 1024 * 1024 * 1024 * 1024
                    else:
                        raise ValueError

                    for _ in range(int(count)):
                        name = generate_unique_name()
                        files.append([name, size, None, 'waiting'])

                    insert_file_info(files)
            except ValueError:
                print("Invalid input. Please enter size and count as integers.")
            break

        orders.append(response.strip())
