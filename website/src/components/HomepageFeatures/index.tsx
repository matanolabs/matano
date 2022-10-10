import React from 'react';
import clsx from 'clsx';
import styles from './styles.module.css';

type FeatureItem = {
  title: string;
  description: JSX.Element;
};

const FeatureList: FeatureItem[] = [
  {
    title: 'Collect data from all your sources',
    description: (
      <>
        Matano lets you collect log data from sources using S3 or SQS based ingestion, and comes out of the box with sources like CloudTrail, Zeek, and more.
      </>
    ),
  },
  {
    title: 'Ingest, transform, normalize log data',
    description: (
      <>
        Matano normalizes and transforms your data using VRL. Matano works with the Elastic Common Schema by default and you can define your own schema.
      </>
    ),
  },
  {
    title: 'Store data in S3 object storage',
    description: (
      <>
        Log data is always stored in highly optimized Parquet files in S3 object storage, for cost effective, long term, durable storage.
      </>
    ),
  },
  {
    title: 'Apache Iceberg Data lake',
    description: (
      <>
        All data is ingested into an Apache Iceberg data lake. The Iceberg open table format ensures you <i>own your data</i> in a vendor agnostic format.
      </>
    ),
  },
  {
    title: 'Serverless',
    description: (
      <>
        Matano is a fully serverless platform, designed for zero-ops and unlimited elastic horizontal scaling.
      </>
    ),
  },
  {
    title: 'Detections as code',
    description: (
      <>
        Write Python detections to implement realtime alerting on your log data.
      </>
    ),
  },
];

function Feature({title, description}: FeatureItem) {
  return (
    <div className={clsx('col col--4')}>
      {/* <div className="text--center">
        <Svg className={styles.featureSvg} role="img" />
      </div> */}
      <div className="text--center padding-horiz--md padding-vert--md">
        <h3 className='text-2xl leading-6'>{title}</h3>
        <p className="text-lg">{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures(): JSX.Element {
  return (
    <section className={styles.features}>
      <div className="container flex flex-col items-center">
        <h2 className="!text-4xl text-center py-8 px-8">
            Why Matano?
        </h2>
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
