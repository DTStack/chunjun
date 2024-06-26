/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  swcMinify: true,
  env: {
    sep: '@',
    root_en: '../docs/docs_en',
    root_zh: '../docs/docs_zh',
    sql: '/chunjun-examples/sql',
    json: '/chunjun-examples/json'
  }
};

const isProd = process.env.NODE_ENV === 'production';

if (isProd) {
  nextConfig.images = {
    loader: 'imgix',
    domains: ['github.com'],
    basePath: '/chunjun',
    path: 'https://dtstack.github.io/chunjun'
  };
  nextConfig.basePath = '/chunjun';
} else {
  nextConfig.experimental = {
    images: {
      unoptimized: true
    }
  };
}

module.exports = nextConfig;
