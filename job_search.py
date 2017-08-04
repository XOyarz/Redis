def add_job(conn, job_id, required_skills):
    conn.sadd('job:' + job_id, *required_skills)

def is_qualified(conn, job_id, candidate_skills):
    temp = str(uuid.uuid4())
    pipeline = conn.pipeline(True)
    pipeline.sadd(temp, *candidate_skills)
    pipeline.expire(temp, 5)
    pipeline.sdiff('job:' + job_id, temp)
    return not pipeline.execute()[-1]

## indexing jobs based on required skills

def index_job(conn, job_id, skills):
    pipeline = conn.pipeline(True)
    for skill in skills:
        pipeline.sadd('idx:skill:' + skill, job_id)
    pipeline.zadd('idx:jobs:req', job_id, len(set(skills)))
    pipeline.execute()


## find all jobs a candidate is qualified for

def find_jobs(conn, candidate_skills):
    skills = {}
    for skill in set(candidate_skills):
        skills['skill:' + skill] = 1

    job_scores = zunion(conn, skills)
    final_result = zintersect(conn, {job_scores:-1, 'jobs:req':1})

    return conn.zrangebyscore('idx:' + final_result, 0, 0)