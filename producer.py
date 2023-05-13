import redis 
import secrets

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

def submit_job(job):
    r = redis.Redis()
    r.rpush('jobs', job)
    print(f'Produced job: {job}')

if __name__ == '__main__':
    while True:
        submit_job(secrets.token_hex(10))