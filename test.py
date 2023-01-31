from prefect import flow, task

@task()
def add_two(a, b):
    return a + b

@flow(og_prints=True)
def test_flow():
    print(f"Number: {add_two(2, 3)}")

if __name__ == "__main__":
    test_flow()