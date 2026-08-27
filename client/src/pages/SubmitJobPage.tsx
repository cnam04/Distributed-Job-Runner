import SiteHeader from '../components/SiteHeader.tsx'

export default function SubmitJobPage() {
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
              <div><span>01</span><p><strong>Submit</strong>Your repository and run command</p></div>
              <div><span>02</span><p><strong>Build</strong>A clean container on the server</p></div>
              <div><span>03</span><p><strong>Monitor</strong>Status and output in one place</p></div>
            </div>
          </section>

          <section className="column is-7">
            <form className="box job-form" action="/jobs/demo-job-42" method="get">
              <div className="form-heading">
                <div>
                  <p className="eyebrow">New workload</p>
                  <h2 className="title is-3">Submit a job</h2>
                </div>
                <span className="draft-badge">Draft mode</span>
              </div>

              <div className="field">
                <label className="label" htmlFor="repository">GitHub repository</label>
                <div className="control">
                  <input
                    className="input"
                    id="repository"
                    name="repository"
                    type="url"
                    defaultValue="https://github.com/student/image-preprocessor"
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

              <div className="field">
                <label className="label" htmlFor="command">Run command</label>
                <div className="control command-control">
                  <span aria-hidden="true">$</span>
                  <input
                    className="input"
                    id="command"
                    name="command"
                    type="text"
                    defaultValue="python process.py /data/input"
                    required
                  />
                </div>
              </div>

              <div className="submission-note">
                <span className="note-dot" aria-hidden="true" />
                <p><strong>Prototype submission</strong>This form opens a dashboard with hardcoded job data.</p>
              </div>

              <button className="button submit-button is-fullwidth" type="submit">
                Submit job <span aria-hidden="true">-&gt;</span>
              </button>
            </form>
          </section>
        </div>
      </main>
    </div>
  )
}
