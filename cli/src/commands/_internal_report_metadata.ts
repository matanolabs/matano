import { Flags } from "@oclif/core";
import BaseCommand from "../base";
import p from "phin";

const METADATA_URL = "https://metadata-reporting.matano.dev/data/";

export default class InternalReportMetadata extends BaseCommand {
  static hidden = true;
  static args = [{ name: "data" }];
  async run() {
    try {
      const data = this.argv?.[0];
      if (data) {
        await p({
          url: METADATA_URL,
          method: "POST",
          data,
        });
      }
    } catch {}
  }
}
