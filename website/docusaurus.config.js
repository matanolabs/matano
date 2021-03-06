// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'Matano',
  tagline: 'Open source security lake platform for AWS',
  url: 'https://matano.dev',
  baseUrl: '/',
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',
  favicon: 'img/favicon.svg',
  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'matanolab', // Usually your GitHub org/user name.
  projectName: 'matano', // Usually your repo name.

  // Even if you don't use internalization, you can use this field to set useful
  // metadata like html lang. For example, if your site is Chinese, you may want
  // to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          editUrl: 'https://github.com/matanolabs/matano/tree/main/website/',
        },
        blog: {
          showReadingTime: true,
          editUrl: 'https://github.com/matanolabs/matano/tree/main/website/',
        },
        theme: {
          customCss: [
            require.resolve('./src/css/custom.css'),
            require.resolve('./src/css/styles.scss'),
          ]
        },
      }),
    ],
  ],

  plugins: [
    'docusaurus-plugin-sass',
    async function tailwindPlugin(context, options) {
      return {
        name: "docusaurus-tailwindcss",
        configurePostCss(postcssOptions) {
          postcssOptions.plugins.push(require("tailwindcss"));
          postcssOptions.plugins.push(require("autoprefixer"));
          return postcssOptions;
        },
      };
    },
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      announcementBar: {
        id: 'mtn-announcement',
        content: '<div id="mtn-announcement">We are looking to revamp our docs, please fill <a target="_blank" rel="noopener noreferrer" href="#">this survey</a></div>',
        backgroundColor: 'var(--ifm-color-primary-light)',
        textColor: '#ffffff',
        isCloseable: true,
      },
      navbar: {
        // title: 'Matano',
        hideOnScroll: true,
        logo: {
          alt: 'Matano',
          src: 'matano-logo/vector/default-monochrome-black.svg',
          srcDark: 'matano-logo/vector/default-monochrome-white.svg',
          style: { height: "30px" },
        },
        items: [
          {
            type: 'doc',
            docId: 'intro',
            position: 'left',
            label: 'Docs',
          },
          // {to: '/blog', label: 'Blog', position: 'left'},
          {
            type: 'custom-githubButton', 
            position: "right",
          },
          {
            href: 'https://github.com/matanolabs/matano',
            label: 'GitHub',
            position: 'right',
          },
        ],
      },
      footer: {
        style: 'dark',
        logo: {
          alt: 'Matano',
          src: 'matano-logo/vector/default-monochrome-white.svg',
          style: { height: "30px" },
        },
        links: [
          {
            title: 'Docs',
            items: [
              {
                label: 'Tutorial',
                to: '/docs/intro',
              },
            ],
          },
          {
            title: 'Community',
            items: [
              {
                label: 'Discord',
                href: 'https://discord.gg/YSYfHMbfZQ',
              },
              {
                label: 'Twitter',
                href: 'https://twitter.com/docusaurus',
              },
            ],
          },
          {
            title: 'More',
            items: [
              {
                label: 'Blog',
                to: '/blog',
              },
              {
                label: 'GitHub',
                href: 'https://github.com/matanolabs/matano',
              },
            ],
          },
        ],
        copyright: `Copyright ?? ${new Date().getFullYear()} Matano Inc.`,
      },
      prism: {
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
      },
    }),
};

module.exports = config;
