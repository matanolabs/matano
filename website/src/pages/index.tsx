import React from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import Head from '@docusaurus/Head';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import CodeBlock from '@theme/CodeBlock';
import HomepageFeatures from '@site/src/components/HomepageFeatures';

import styles from './index.module.css';


// h-screen
function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className="hero-bg flex px-12 sm:px-24 py-12 sm:py-20">
      <div className="">
        <h1 className="pt-0 subpixel-antialiased mx-auto max-w-7xl font-display sm:text-6xl text-5xl font-semibold tracking-tight text-slate-900 " 
        style={{
          fontFamily: "Lexend",
          lineHeight: 1.1,
        }}
        >The Open Source<br/><span className="" style={{color: "var(--ifm-color-primary-dark)"}}>Security Lake Platform</span> for AWS</h1>
        <p 
        // style={{
        //   fontFamily: "Inter",
        //   fontSize: "2rem",
        //   lineHeight: 1.1,
        //   marginTop: "1.5rem",
        // }}
        className="hero__subtitle">You are super duper secure in the multi cloud on our serverless Rust lake.</p>
        <div>
          <a className="my-btn text-white text-lg bg-blue-600 hover:bg-blue-700 w-full mb-2 sm:w-auto" href="https://dashboard.apptrail.com/signup" style={{outline: "none"}}>Get started</a>
          <a className="my-btn border-2 text-blue-600 text-lg border-blue-600 hover:bg-blue-100 w-full sm:ml-4 mb-2 sm:w-auto tw-border-solid" href="https://apptrail.com/docs/applications/guide" style={{outline: "none"}}>Explore Docs</a>
        </div>
        
        {/* <div className="flex items-center justify-center gap-5 mt-5">
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
        </div> */}
      </div>
    </header>
  );
}

export default function Home(): JSX.Element {
  const { siteConfig } = useDocusaurusContext();
  return (
    <Layout wrapperClassName=''>
        <Head>
          <title>Matano | Open source security lake for AWS</title>
        </Head>
        <div className=''>
          <HomepageHeader />
        </div> 
        <main style={{backgroundColor: undefined}}>
          <HomepageFeatures />
        </main>
    </Layout>
  );
}
