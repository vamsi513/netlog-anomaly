import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  reactStrictMode: true,
  // Next writes a generated AGENTS.md into the project root on dev startup
  // unless this is disabled. It is not part of this project's documentation.
  agentRules: false,
};

export default nextConfig;
