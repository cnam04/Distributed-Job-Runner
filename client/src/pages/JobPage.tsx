import SiteHeader from '../components/SiteHeader.tsx'

export default function JobPage() {
  return (
    <div className="app-shell">
      <SiteHeader current="job" />

      <main className="container is-max-desktop page-content job-page">
        <div className="job-title-row">
          <div>
            <a className="back-link" href="/">&lt;- All jobs</a>
            <p className="eyebrow">Job / demo-job-42</p>
            <h1 className="title is-2">image-preprocessor</h1>
          </div>
          <div className="running-badge"><span /> Running</div>
        </div>

        <section className="box status-card" aria-labelledby="progress-heading">
          <div className="status-card-heading">
            <div>
              <p className="eyebrow">Current stage</p>
              <h2 className="title is-4" id="progress-heading">Processing workload</h2>
            </div>
            <p className="elapsed"><span>Elapsed</span> 00:04:18</p>
          </div>

          <div className="pipeline">
            <div className="pipeline-step is-complete"><span>1</span><p>Queued<small>10:32:04</small></p></div>
            <div className="pipeline-step is-complete"><span>2</span><p>Cloning<small>10:32:07</small></p></div>
            <div className="pipeline-step is-complete"><span>3</span><p>Building<small>10:32:19</small></p></div>
            <div className="pipeline-step is-active"><span>4</span><p>Running<small>10:33:51</small></p></div>
            <div className="pipeline-step"><span>5</span><p>Complete<small>Pending</small></p></div>
          </div>
        </section>

        <div className="columns is-variable is-5 job-grid">
          <section className="column is-8">
            <div className="terminal">
              <div className="terminal-bar">
                <div className="terminal-dots" aria-hidden="true"><i /><i /><i /></div>
                <p>Live output</p>
                <span>stdout</span>
              </div>
              <pre aria-label="Standard output"><code><b>10:33:51</b>  Container started on worker-03{`\n`}<b>10:33:52</b>  Found 50,000 images in /data/input{`\n`}<b>10:33:52</b>  Splitting input into 50 chunks{`\n`}<b>10:33:54</b>  [########------------] chunk 21/50{`\n`}<b>10:35:17</b>  Processed 21,000 / 50,000 images{`\n`}<b>10:36:22</b>  [############--------] chunk 31/50{`\n`}<span className="terminal-cursor"> </span></code></pre>
            </div>

            <details className="stderr-panel">
              <summary>stderr <span>0 messages</span></summary>
              <p>No error output has been reported.</p>
            </details>
          </section>

          <aside className="column is-4" aria-label="Job details">
            <div className="box details-card">
              <p className="eyebrow">Job details</p>
              <dl>
                <div><dt>Repository</dt><dd><a href="https://github.com/student/image-preprocessor">student/image-preprocessor</a></dd></div>
                <div><dt>Dockerfile</dt><dd><code>./Dockerfile</code></dd></div>
                <div><dt>Command</dt><dd><code>python process.py /data/input</code></dd></div>
                <div><dt>Created</dt><dd>Today, 10:32 AM</dd></div>
                <div><dt>Worker</dt><dd>worker-03</dd></div>
              </dl>
            </div>
          </aside>
        </div>
      </main>
    </div>
  )
}
