from create_combined import ProcessIpAddresses


def main():
    dir_path = None  # input("Enter path to processed:")
    dir_path = dir_path.strip() if dir_path else None
    process1 = ProcessIpAddresses(dir_path)
    process1.process_files()


if __name__ == "__main__":
    main()
