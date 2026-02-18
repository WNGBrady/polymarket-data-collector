import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  async rewrites() {
    return [
      {
        source: "/api/:path*",
        destination: "http://159.203.33.91/api/:path*",
      },
    ];
  },
};

export default nextConfig;
