import JobPage from './pages/JobPage.tsx'
import SubmitJobPage from './pages/SubmitJobPage.tsx'

export default function App() {
  return window.location.pathname.startsWith('/jobs/') ? <JobPage /> : <SubmitJobPage />
}
