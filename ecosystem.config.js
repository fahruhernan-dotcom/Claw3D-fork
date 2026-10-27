module.exports = {
  apps: [
    {
      name: "claw3d-adapter",
      cwd: "/home/ubuntu/apps/claw3d",
      script: "server/vps-brain-adapter.js",
      max_memory_restart: "200M",
      env: {
        NODE_ENV: "production",
      },
    },
    {
      name: "claw3d-web",
      cwd: "/home/ubuntu/apps/claw3d",
      script: "server/index.js",
      max_memory_restart: "350M",
      env: {
        NODE_ENV: "production",
        PORT: "3000",
        HOST: "127.0.0.1",
        UPSTREAM_ALLOWLIST: "localhost,127.0.0.1",
      },
    },
  ],
};
