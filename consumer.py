import redis
import time
import argparse

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

polling_wait = 0.5

def process_jobs():
    while True:
        job = r.blpop('jobs') 
        print(f'Processing job: {job}')

def process_jobs_polling():
    while True:
        job = r.lpop('jobs') 

        if job:
            print(f'Processing job: {job}')
        else:
            time.sleep(polling_wait)

# we would like to atomic push to processing queue, so if we stop while procesing, we can return to the processing queue later
def safe_process_jobs(worker_id):
    while True:
        job = r.brpoplpush('jobs', f'{worker_id}_processing')
        print(f'Processing job: {job}')
        r.lrem(f'{worker_id}_processing', 0, job)

# what I have noticed is that polling is a little slower when the consumer is faster than producer maybe smaller waiting time would help
def safe_process_jobs_polling(worker_id):
    while True:
        job = r.rpoplpush('jobs', f'{worker_id}_processing')

        if job:
            print(f'Processing job: {job}')
            r.lrem(f'{worker_id}_processing', 0, job)
        else:
            time.sleep(polling_wait)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
                    prog='Redis consumer',
                    description='Takes jobs out of redis DB and process them.')
    
    parser.add_argument('worker_id')
    parser.add_argument('-s', '--safe', action='store_true')
    parser.add_argument('-p', '--polling', action='store_true')

    args = parser.parse_args()

    worker_id = args.worker_id

    if args.safe:
        if args.polling:
            safe_process_jobs_polling(worker_id)
        else:
            safe_process_jobs(worker_id)
    else:
        if args.polling:
            process_jobs_polling()
        else:
            process_jobs()
    
    