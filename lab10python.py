import time

data = [
    (1, "Alice", 23),
    (2, "Bob", 17),
    (3, "Charlie", 35),
    (4, "David", 16),
    (5, "Eva", 29)
]

start = time.time()

filtered = [person for person in data if person[2] > 18]

average = sum(person[2] for person in filtered) / len(filtered)

end = time.time()

print("Average age:", average)
print("Execution time:", end - start)
