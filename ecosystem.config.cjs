module.exports = {
  apps: [
    {
      name: 'memento-node',
      script: './target/release/memento',
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '1G',
      env: {
        NODE_ENV: 'development',
        RUST_LOG: 'info'
      },
      env_production: {
        NODE_ENV: 'production',
        RUST_LOG: 'info'
      }
    }
  ]
};
