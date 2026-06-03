import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import App from '../../../dashboards/kafka-tcp-tuning.jsx'

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <App />
  </StrictMode>
)
