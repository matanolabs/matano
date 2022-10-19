import ComponentTypes from "@theme-original/NavbarItem/ComponentTypes";
import GithubButton from "@site/src/components/NavbarItems/GithubButton";

// this is needed currently (07/2022) to add a custom component to the navbar
// (see https://github.com/facebook/docusaurus/issues/7227).
export default {
  ...ComponentTypes,
  "custom-githubButton": GithubButton,
};
