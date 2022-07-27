import React from "react";
import styles from './styles.module.css';

export default function GithubButton() {
    return <iframe
        className={styles.ghButton}
        src="https://ghbtns.com/github-btn.html?user=matanolabs&repo=matano&type=star&count=true&size=large" 
        frameBorder="0"
        scrolling="0"
        width="115"
        height="30"
        title="GitHub"
    ></iframe>;
}
