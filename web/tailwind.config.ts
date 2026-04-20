import type { Config } from "tailwindcss";

const config: Config = {
  content: ["./app/**/*.{ts,tsx}", "./components/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        bg: "#0b0d10",
        panel: "#111418",
        border: "#1f242b",
        muted: "#8a94a3",
        text: "#e6eaf0",
        accent: "#4f8cff",
        good: "#21c07a",
        warn: "#f5a524",
        bad: "#f26d6d",
      },
    },
  },
  plugins: [],
};
export default config;
