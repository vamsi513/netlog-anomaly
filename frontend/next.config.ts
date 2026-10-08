import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  reactStrictMode: true,
  // Next writes an AGENTS.md into the project on dev startup unless this is
  // off. Nothing here needs it and it is not part of the project's docs.
  agentRules: false,
};

export default nextConfig;
