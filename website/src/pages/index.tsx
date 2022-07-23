import React from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import Head from '@docusaurus/Head';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import CodeBlock from '@theme/CodeBlock';
import HomepageFeatures from '@site/src/components/HomepageFeatures';

import styles from './index.module.css';

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className="flex justify-center px-16 py-12 h-screen px-16 text-center">
      <div className="">
        <h1 className="subpixel-antialiased	mx-auto max-w-7xl font-display sm:text-7xl text-5xl font-semibold tracking-tight text-slate-900 " 
        style={{
          fontFamily: "Lexend",
          lineHeight: 1.1,
        }}
        >The Open Source<br/><span className="" style={{color: "var(--ifm-color-primary)"}}>Security Lake Platform</span> for AWS</h1>
        <p 
        // style={{
        //   fontFamily: "Inter",
        //   fontSize: "2rem",
        //   lineHeight: 1.1,
        //   marginTop: "1.5rem",
        // }}
        className="hero__subtitle">You are super duper secure in the multi cloud on our serverless Rust lake.</p>
        <div className="flex items-center justify-center gap-5 mt-5">
          <Link
            className="button button--secondary button--lg"
            to="/docs/intro">
            Learn more
          </Link>
          <Link
            className="button button--primary button--lg"
            href="https://github.com/matanolabs/matano#--">
            Get started
          </Link>
        </div>
      </div>
    </header>
  );
}

export default function Home(): JSX.Element {
  const { siteConfig } = useDocusaurusContext();
  return (
    <Layout>
      <Head>
        <title>Matano | Open source security lake for AWS</title>
      </Head>
      <HomepageHeader />
      {/* <main>
        <HomepageFeatures />
      </main> */}
    </Layout>
  );
}
