/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        nexus: {
          950: '#07090e',
          900: '#0d1117',
          850: '#131822',
          800: '#161b26',
          700: '#212838',
          600: '#2d374d',
          accent: '#38bdf8',
          accentDark: '#0284c7',
          success: '#10b981',
          warning: '#f59e0b',
          danger: '#ef4444'
        }
      }
    },
  },
  plugins: [],
}
