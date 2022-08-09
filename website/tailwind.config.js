module.exports = {
  content: ["./src/**/*.{jsx,ts,tsx}"],
  darkMode: "class",
  theme: {
    extend: {
      backgroundImage: {
        "hero-pattern": "url('./img/bg.png')",
        block: "url('./img/block2x.png')",
        block2: "url('./img/block2@2x.png')",
        wave: "url(./img/wave2.svg)",
        radar: "url(./img/radar.svg)",
        cube: "url(./img/cube.svg)",
      },
    },
  },
  plugins: [],
}
