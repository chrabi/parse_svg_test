#!/usr/bin/env python3
"""
Test script to verify multithreading functionality
Compatible with Python 3.6.8
"""

import threading
import time
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s')
LOG = logging.getLogger(__name__)

def chunk_list(lst, chunk_size):
    """Divide list into chunks of specified size"""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

def simulate_serial_processing(serial_number, duration=2):
    """Simulate processing of a single serial number"""
    thread_name = threading.current_thread().name
    LOG.info(f"[{thread_name}] Starting processing serial: {serial_number}")
    
    # Simulate some work
    time.sleep(duration)
    
    LOG.info(f"[{thread_name}] Completed processing serial: {serial_number}")
    return f"Result for {serial_number}"

def process_serial_chunk(serial_chunk, duration=2):
    """Process a chunk of serial numbers in parallel"""
    chunk_name = f"Chunk-{'-'.join(serial_chunk)}"
    LOG.info(f"[{chunk_name}] Starting processing chunk: {serial_chunk}")
    
    # Create threads for each serial number in the chunk
    threads = []
    results = []
    
    for serial_number in serial_chunk:
        thread = threading.Thread(
            target=lambda sn=serial_number: results.append(simulate_serial_processing(sn, duration)),
            name=f"Serial-{serial_number}"
        )
        threads.append(thread)
        thread.start()
    
    # Wait for all threads in this chunk to complete
    for thread in threads:
        thread.join()
    
    LOG.info(f"[{chunk_name}] Completed processing chunk: {serial_chunk}")
    return results

def test_multithreading():
    """Test the multithreading functionality"""
    LOG.info("Starting multithreading test")
    
    # Simulate serial numbers like in the real script
    serial_numbers = ["56456", "56888", "56600", "57000", "38000", "59000", "53456", "52345"]
    
    LOG.info(f"Found {len(serial_numbers)} serial numbers: {serial_numbers}")
    
    # Process serial numbers in chunks of 2
    chunk_size = 2
    serial_chunks = list(chunk_list(serial_numbers, chunk_size))
    
    LOG.info(f"Processing {len(serial_chunks)} chunks of {chunk_size} serial numbers each")
    
    start_time = time.time()
    
    # Process each chunk sequentially (but within each chunk, process in parallel)
    all_results = []
    for i, chunk in enumerate(serial_chunks):
        LOG.info(f"Processing chunk {i+1}/{len(serial_chunks)}: {chunk}")
        chunk_results = process_serial_chunk(chunk, duration=2)
        all_results.extend(chunk_results)
    
    end_time = time.time()
    
    LOG.info(f"Multithreading test completed in {end_time - start_time:.2f} seconds")
    LOG.info(f"Results: {all_results}")
    
    # Compare with sequential processing
    LOG.info("Starting sequential test for comparison")
    start_time = time.time()
    
    sequential_results = []
    for serial in serial_numbers:
        result = simulate_serial_processing(serial, duration=2)
        sequential_results.append(result)
    
    end_time = time.time()
    
    LOG.info(f"Sequential test completed in {end_time - start_time:.2f} seconds")
    LOG.info(f"Sequential results: {sequential_results}")

if __name__ == "__main__":
    test_multithreading()
