/** @type {import('tailwindcss').Config} */
module.exports = {
  mode: 'jit',
  content: [
    './pages/**/*.{js,ts,jsx,tsx}',
    './components/**/*.{js,ts,jsx,tsx}',
    './common/**/*.{js,ts,jsx,tsx}'
  ],
  theme: {
    extend: {
      backgroundImage: {
        'data-light': "url('/assets/svg/cdp-data-light.svg')",
        'data-dark': "url('/assets/svg/cdp-data-dark.svg')"
      }
    }
  },
  plugins: [],
  darkMode: 'class'
}
