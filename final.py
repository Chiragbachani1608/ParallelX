import threading
import os
import queue
import time

# Queues for inter-thread communication
file_chunks_queue = queue.Queue()   # To hold file chunks to be processed
results_queue = queue.Queue()       # To hold results after processing

# File Service (Simulated Microservice 1) - Reads file chunks
def file_service(file_name, chunk_size, num_chunks):
    with open(file_name, 'r') as file:
        for i in range(num_chunks):
            start = i * chunk_size
            file.seek(start)
            chunk = file.read(chunk_size)
            file_chunks_queue.put(chunk)
            print(f"File Service: Queued chunk {i + 1} for processing.")
        # Signal no more chunks
        file_chunks_queue.put(None)

# Processing Service (Simulated Microservice 2) - Processes file chunks
def processing_service():
    while True:
        chunk = file_chunks_queue.get()
        if chunk is None:
            file_chunks_queue.put(None)  # Signal to other threads
            break
        word_count = len(chunk.split())
        print(f"Processing Service: Processed chunk with {word_count} words.")
        results_queue.put(word_count)

# Storage Service (Simulated Microservice 3) - Stores processed results
def storage_service():
    total_word_count = 0
    while True:
        result = results_queue.get()
        if result is None:
            break
        total_word_count += result
        print(f"Storage Service: Stored result. Total words so far: {total_word_count}")
    print(f"Final word count: {total_word_count}")

# Orchestrator - Controls the flow of the application
def orchestrator(file_name, num_threads=4):
    file_size = os.path.getsize(file_name)
    chunk_size = file_size // num_threads

    # Start the File Service
    file_thread = threading.Thread(target=file_service, args=(file_name, chunk_size, num_threads))
    
    # Start multiple Processing Services (each in its own thread)
    processing_threads = []
    for _ in range(num_threads):
        t = threading.Thread(target=processing_service)
        processing_threads.append(t)
        t.start()

    # Start the Storage Service
    storage_thread = threading.Thread(target=storage_service)

    # Run all services
    file_thread.start()
    storage_thread.start()

    # Wait for file service to finish
    file_thread.join()

    # Wait for processing services to finish
    for t in processing_threads:
        t.join()

    # Signal storage service to finish
    results_queue.put(None)
    
    # Wait for storage service to finish
    storage_thread.join()

if __name__ == "__main__":
    # Use the full path to the downloaded file
    file_name = 'C:/Users/chira/OneDrive/Desktop/ParallelX/large_file.txt'

    start_time = time.time()
    orchestrator(file_name, num_threads=4)
    end_time = time.time()
    print(f"Processing completed in {end_time - start_time:.2f} seconds.")
