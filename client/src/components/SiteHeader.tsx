type SiteHeaderProps = {
  current: 'submit' | 'job'
}

export default function SiteHeader({ current }: SiteHeaderProps) {
  return (
    <header className="site-header">
      <div className="container is-max-desktop header-inner">
        <a className="brand" href="/" aria-label="Campus Compute home">
          <span className="brand-mark" aria-hidden="true">
            <i />
            <i />
            <i />
            <i />
          </span>
          <span>Campus Compute</span>
        </a>

        <nav className="header-nav" aria-label="Primary navigation">
          <a className={current === 'submit' ? 'is-current' : ''} href="/">
            Submit job
          </a>
          <a className={current === 'job' ? 'is-current' : ''} href="/jobs/demo-job-42">
            Demo job
          </a>
        </nav>
      </div>
    </header>
  )
}
