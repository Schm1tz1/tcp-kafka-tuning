import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  base: '/tcp-kafka-tuning/kafka/',
  build: {
    outDir: 'dist',
    emptyOutDir: true,
  },
})
