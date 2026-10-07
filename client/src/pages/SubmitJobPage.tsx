import { useState, type FormEvent } from 'react'
import SiteHeader from '../components/SiteHeader.tsx'

export default function SubmitJobPage() {
  const [error, setError] = useState('')
  const [submitting, setSubmitting] = useState(false)

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    setError('')
    setSubmitting(true)
    const data = new FormData(event.currentTarget)
    try {
      const response = await fetch('/api/jobs', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          repoUrl: data.get('repository'),
          dockerfilePath: data.get('dockerfile'),
        }),
      })
      if (!response.ok) throw new Error((await response.text()) || 'Could not submit job')
      const job = await response.json() as { id: string }
      window.location.assign(`/jobs/${encodeURIComponent(job.id)}`)
    } catch (error) {
      setError(error instanceof Error ? error.message : 'Could not submit job')
      setSubmitting(false)
    }
  }

  return (
    <div className="app-shell">
      <SiteHeader current="submit" />

      <main className="container is-max-desktop page-content">
        <div className="columns is-variable is-8">
          <section className="column is-5 intro-panel">
            <p className="eyebrow">SUNY New Paltz / CS</p>
            <h1 className="title is-1">Run the work.<br />Not your laptop.</h1>
            <p className="intro-copy">
              Send a containerized workload to shared campus servers and follow it from build to completion.
            </p>

            <div className="process-list" aria-label="Job process">
              <div><span>01</span><p><strong>Submit</strong>Your repository and Dockerfile</p></div>
              <div><span>02</span><p><strong>Build</strong>A clean container on the server</p></div>
              <div><span>03</span><p><strong>Monitor</strong>Status in the dashboard</p></div>
            </div>
          </section>

          <section className="column is-7">
            <form className="box job-form" onSubmit={submit}>
              <div className="form-heading">
                <div>
                  <p className="eyebrow">New workload</p>
                  <h2 className="title is-3">Submit a job</h2>
                </div>
                <span className="draft-badge">Demo mode</span>
              </div>

              <div className="field">
                <label className="label" htmlFor="repository">GitHub repository</label>
                <div className="control command-control">
                  <input
                    className="input"
                    id="repository"
                    name="repository"
                    type="url"
                    defaultValue="https://github.com/cnam04/test_repo"
                    required
                  />
                </div>
                <p className="help">The repository must include everything needed to build the job.</p>
              </div>

              <div className="field">
                <label className="label" htmlFor="dockerfile">Dockerfile path</label>
                <div className="control path-control">
                  <span aria-hidden="true">repo /</span>
                  <input
                    className="input"
                    id="dockerfile"
                    name="dockerfile"
                    type="text"
                    defaultValue="Dockerfile"
                    required
                  />
                </div>
              </div>

              <div className="submission-note">
                <span className="note-dot" aria-hidden="true" />
                <p><strong>Remote execution</strong>The coordinator builds this repository and sends it to the configured worker.</p>
              </div>

              {error && <p className="help is-danger mb-4" role="alert">{error}</p>}

              <button className={`button submit-button is-fullwidth${submitting ? ' is-loading' : ''}`} type="submit" disabled={submitting}>
                Submit job <span aria-hidden="true">-&gt;</span>
              </button>
            </form>
          </section>
        </div>
      </main>
    </div>
  )
}
