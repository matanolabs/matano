import { Command, Help } from '@oclif/core';
import chalk from 'chalk';

const LOGO = ``;

const l1 = `\
█▀▄▀█ ▄▀█ ▀█▀ ▄▀█ █▄░█ █▀█
█░▀░█ █▀█ ░█░ █▀█ █░▀█ █▄█`;

export default class extends Help {
  protected formatRoot(): string {
    const baseRoot = super.formatRoot();
    return chalk.blue(l1) + "\n\n" + baseRoot;
  }
}