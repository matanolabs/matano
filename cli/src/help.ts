import { Command, Help } from "@oclif/core";
import chalk from "chalk";

export const LOGO = `\
█▀▄▀█ ▄▀█ ▀█▀ ▄▀█ █▄░█ █▀█
█░▀░█ █▀█ ░█░ █▀█ █░▀█ █▄█`;

export default class extends Help {
  protected formatRoot(): string {
    const baseRoot = super.formatRoot();
    return chalk.blueBright(LOGO) + "\n\n" + baseRoot;
  }
}
