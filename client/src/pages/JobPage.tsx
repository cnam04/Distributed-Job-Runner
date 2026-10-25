import { useEffect, useState } from 'react'
import SiteHeader from '../components/SiteHeader.tsx'

type JobStatus = 'queued' | 'cloning' | 'building' | 'running' | 'completed' | 'failed'

type TaskLog = {
  taskId: string
  logs: string
}

type WorkerLog = {
  workerId: number
  tasks: TaskLog[]
}

type Job = {
  id: string
  repoUrl: string
  dockerfilePath: string
  imageRef: string
  status: JobStatus
  workerLogs: WorkerLog[]
}

const pipeline: JobStatus[] = ['queued', 'cloning', 'building', 'running', 'completed']

export default function JobPage() {
  const id = decodeURIComponent(window.location.pathname.slice('/jobs/'.length))
  const [job, setJob] = useState<Job | null>(null)
  const [error, setError] = useState('')
  const [activeWorkerId, setActiveWorkerId] = useState<number | null>(null)

  useEffect(() => {
    let timer = 0
    let stopped = false
    async function poll() {
      try {
        const response = await fetch(`/api/jobs/${encodeURIComponent(id)}`)
        if (!response.ok) throw new Error((await response.text()) || 'Could not load job')
        const nextJob = await response.json() as Job
        if (stopped) return
        setJob(nextJob)
        if (nextJob.status !== 'completed' && nextJob.status !== 'failed') {
          timer = window.setTimeout(poll, 1000)
        }
      } catch (error) {
        if (!stopped) setError(error instanceof Error ? error.message : 'Could not load job')
      }
    }
    void poll()
    return () => {
      stopped = true
      window.clearTimeout(timer)
    }
  }, [id])

  const currentStep = job ? pipeline.indexOf(job.status) : -1
  const title = job?.repoUrl.split('/').filter(Boolean).at(-1) ?? 'Loading job'
  const activeWorker = job?.workerLogs.find(({ workerId }) => workerId === activeWorkerId) ?? job?.workerLogs[0]
  const workerOutput = activeWorker?.tasks.map(({ taskId, logs }) => `[Task ${taskId}]\n${logs || 'Waiting for output...\n'}`).join('\n')

  return (
    <div className="app-shell">
      <SiteHeader current="job" />

      <main className="container is-max-desktop page-content job-page">
        <div className="job-title-row">
          <div>
            <a className="back-link" href="/">&lt;- All jobs</a>
            <p className="eyebrow">Job / {id}</p>
            <h1 className="title is-2">{title}</h1>
          </div>
          <div className="running-badge"><span /> {job?.status ?? 'loading'}</div>
        </div>

        {error && <div className="notification is-danger" role="alert">{error}</div>}

        <section className="box status-card" aria-labelledby="progress-heading">
          <div className="status-card-heading">
            <div>
              <p className="eyebrow">Current stage</p>
              <h2 className="title is-4" id="progress-heading">{job ? job.status : 'Loading status'}</h2>
            </div>
            <p className="elapsed">Updates every second because I'm just polling for now</p>
          </div>

          <div className="pipeline">
            {pipeline.map((status, index) => {
              const complete = job?.status === 'completed' || currentStep > index
              const active = currentStep === index
              return (
                <div className={`pipeline-step${complete ? ' is-complete' : ''}${active ? ' is-active' : ''}`} key={status}>
                  <span>{index + 1}</span>
                  <p>{status}<small>{complete ? 'Done' : active ? 'Current' : 'Pending'}</small></p>
                </div>
              )
            })}
          </div>
        </section>

        <div className="columns is-variable is-5 job-grid">
          <section className="column is-8">
            <div className="worker-tabs" aria-label="Workers">
              {job?.workerLogs.map(({ workerId }) => (
                <button
                  className={activeWorker?.workerId === workerId ? 'is-active' : ''}
                  type="button"
                  aria-pressed={activeWorker?.workerId === workerId}
                  key={workerId}
                  onClick={() => setActiveWorkerId(workerId)}
                >
                  Worker {workerId + 1}
                </button>
              ))}
            </div>
            <div className="terminal">
              <div className="terminal-bar">
                <div className="terminal-dots" aria-hidden="true"><i /><i /><i /></div>
                <p>Live output</p>
                <span>stdout + stderr</span>
              </div>
              <pre aria-label="Worker output">
                <code>
                  {workerOutput || 'Waiting for worker output...\n'}{job?.status !== 'completed' && job?.status !== 'failed' && <span className="terminal-cursor"> </span>}
                </code>
              </pre>
            </div>
          </section>

          <aside className="column is-4" aria-label="Job details">
            <div className="box details-card">
              <p className="eyebrow">Job details</p>
              <dl>
                <div><dt>Repository</dt><dd>{job ? <a href={job.repoUrl}>{job.repoUrl}</a> : 'Loading'}</dd></div>
                <div><dt>Dockerfile</dt><dd><code>{job?.dockerfilePath ?? 'Loading'}</code></dd></div>
                <div><dt>Image</dt><dd><code>{job?.imageRef || 'Pending'}</code></dd></div>
               </dl>
            </div>
          </aside>
        </div>
      </main>
    </div>
  )
}
