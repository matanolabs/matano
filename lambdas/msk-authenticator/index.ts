import { Mechanism, SaslAuthenticationRequest, SaslAuthenticationResponse } from "kafkajs";

import { Hmac, createHmac, createHash, BinaryLike, KeyObject, Encoding } from "crypto";
import { SignatureV4 } from "@aws-sdk/signature-v4";
import { defaultProvider } from "@aws-sdk/credential-provider-node";
import { getDefaultRoleAssumerWithWebIdentity } from "@aws-sdk/client-sts";
import { MemoizedProvider, Hash, SourceData, Credentials } from "@aws-sdk/types";


const SERVICE = 'kafka-cluster'
const SIGNED_HEADERS = 'host'
const HASHED_PAYLOAD = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
const ALGORITHM = 'AWS4-HMAC-SHA256'
const ACTION = 'kafka-cluster:Connect'

interface AuthentcationPayloadCreatorProps {
  region: string;
  ttl?: string;
  userAgent?: string;
}
class AuthenticationPayloadCreator {
  region: string;
  ttl: string;
  userAgent: string;
  provider: MemoizedProvider<Credentials>;
  signature: SignatureV4;

  constructor ({ region, ttl, userAgent }: AuthentcationPayloadCreatorProps) {
    this.region = region
    this.ttl = ttl || '900'
    this.userAgent = userAgent || 'MSK_IAM_v1.0.0'
    this.provider = defaultProvider({
      roleAssumerWithWebIdentity: getDefaultRoleAssumerWithWebIdentity()
    })

    this.signature = new SignatureV4({
      credentials: this.provider,
      region: this.region,
      service: SERVICE,
      applyChecksum: false,
      uriEscapePath: true,
      sha256: Sha256HashConstructor as any
    })
  }

  timestampYYYYmmDDFormat (date: string | number | Date) {
    const d = new Date(date)
    return this.timestampYYYYmmDDTHHMMSSZFormat(d).substring(0, 8)
  }

  timestampYYYYmmDDTHHMMSSZFormat (date: string | number | Date) {
    const d = new Date(date)
    return d.toISOString()
      .replace(/[-.:]/g, '')
      .substring(0, 15)
      .concat('Z')
  }

  generateCanonicalHeaders (brokerHost: any) {
    return `host:${brokerHost}\n`
  }

  generateXAmzCredential (accessKeyId: string, dateString: string) {
    return `${accessKeyId}/${dateString}/${this.region}/${SERVICE}/aws4_request`
  }

  generateStringToSign (date: number, canonicalRequest: string) {
    return `${ALGORITHM}
${this.timestampYYYYmmDDTHHMMSSZFormat(date)}
${this.timestampYYYYmmDDFormat(date)}/${this.region}/${SERVICE}/aws4_request
${createHash('sha256').update(canonicalRequest, 'utf8').digest('hex')}`
  }

  generateCanonicalQueryString (dateString: string | number | boolean, xAmzCredential: string | number | boolean, sessionToken: string | number | boolean | undefined) {
    let canonicalQueryString = ''
    canonicalQueryString += `${encodeURIComponent('Action')}=${encodeURIComponent(ACTION)}&`
    canonicalQueryString += `${encodeURIComponent('X-Amz-Algorithm')}=${encodeURIComponent(ALGORITHM)}&`
    canonicalQueryString += `${encodeURIComponent('X-Amz-Credential')}=${encodeURIComponent(xAmzCredential)}&`
    canonicalQueryString += `${encodeURIComponent('X-Amz-Date')}=${encodeURIComponent(dateString)}&`
    canonicalQueryString += `${encodeURIComponent('X-Amz-Expires')}=${encodeURIComponent(this.ttl)}&`

    if (sessionToken)
      canonicalQueryString += `${encodeURIComponent('X-Amz-Security-Token')}=${encodeURIComponent(sessionToken)}&`

    canonicalQueryString += `${encodeURIComponent('X-Amz-SignedHeaders')}=${encodeURIComponent(SIGNED_HEADERS)}`

    return canonicalQueryString
  }

  generateCanonicalRequest (canonicalQueryString: string, canonicalHeaders: string, signedHeaders: string, hashedPayload: string) {
    return 'GET\n' +
          '/\n' +
          canonicalQueryString + '\n' +
          canonicalHeaders + '\n' +
          signedHeaders + '\n' +
          hashedPayload
  };

  // TESTED
  async create ({ brokerHost }: { brokerHost: string | undefined }) {
    if (!brokerHost) {
      throw new Error('Missing values')
    }

    const { accessKeyId, sessionToken } = await this.provider()

    const now = Date.now()

    const xAmzCredential = this.generateXAmzCredential(accessKeyId, this.timestampYYYYmmDDFormat(now))
    const canonicalHeaders = this.generateCanonicalHeaders(brokerHost)
    const canonicalQueryString = this.generateCanonicalQueryString(this.timestampYYYYmmDDTHHMMSSZFormat(now), xAmzCredential, sessionToken)
    const canonicalRequest = this.generateCanonicalRequest(canonicalQueryString, canonicalHeaders, SIGNED_HEADERS, HASHED_PAYLOAD) //
    const stringToSign = this.generateStringToSign(
      now,
      canonicalRequest
    )

    const signature = await this.signature.sign(stringToSign, {
      signingDate: new Date(now).toISOString()
    })

    return {
      version: '2020_10_22',
      'user-agent': this.userAgent,
      host: brokerHost,
      action: ACTION,
      'x-amz-credential': xAmzCredential,
      'x-amz-algorithm': ALGORITHM,
      'x-amz-date': this.timestampYYYYmmDDTHHMMSSZFormat(now),
      'x-amz-security-token': sessionToken,
      'x-amz-signedheaders': SIGNED_HEADERS,
      'x-amz-expires': this.ttl,
      'x-amz-signature': signature
    }
  }
}


class Sha256HashConstructor {
  sha256: Hmac;

  constructor(signingKey: BinaryLike | KeyObject) {
    this.sha256 = createHmac("sha256", signingKey);
  }

  digest() {
    return Promise.resolve(this.sha256.digest());
  }

  update(toHash: BinaryLike, encoding: Encoding) {
    if (typeof toHash === "string" && encoding !== undefined) {
      this.sha256.update(toHash, encoding);
      return;
    }

    this.sha256.update(toHash);
  }
}

export const awsIamAuthenticator: (
  region: string,
  ttl?: string,
) => Mechanism["authenticationProvider"] = (region, ttl) => (host, port, logger, saslAuthenticate) => {
  const INT32_SIZE = 4

  const requester = (payload: { version: string; 'user-agent': string; host: string; action: string; 'x-amz-credential': string; 'x-amz-algorithm': string; 'x-amz-date': string; 'x-amz-security-token': string | undefined; 'x-amz-signedheaders': string; 'x-amz-expires': string; 'x-amz-signature': string; }) => ({
    encode: () => {
      const stringifiedPayload = JSON.stringify(payload)
      const byteLength = Buffer.byteLength(stringifiedPayload, 'utf8')
      const buf = Buffer.alloc(INT32_SIZE + byteLength)
      buf.writeUInt32BE(byteLength, 0)
      buf.write(stringifiedPayload, INT32_SIZE, byteLength, 'utf8')
      return buf
    }
  })

  const response = {
    decode: (rawData: { readInt32BE: (arg0: number) => any; slice: (arg0: number, arg1: any) => any; }) => {
      const byteLength = rawData.readInt32BE(0)
      return rawData.slice(INT32_SIZE, INT32_SIZE + byteLength)
    },

    parse: (data: { toString: () => string; }) => {
      return JSON.parse(data.toString())
    }
  }

  return {
    authenticate: async () => {
      const broker = `${host}:${port}`
      const payloadFactory = new AuthenticationPayloadCreator({ region, ttl })

      try {
        const payload = await payloadFactory.create({ brokerHost: host })
        const request = requester(payload);
        const authenticateResponse: any = await saslAuthenticate({request, response} as any)
        logger.info('Authentication response', { authenticateResponse })

        if (!authenticateResponse.version || !authenticateResponse) {
          throw new Error('Invalid response from broker')
        }

        logger.info('SASL Simon authentication successful', { broker })
      } catch (error: any) {
        logger.error(error.message, { broker })
        throw error
      }
    }
  }
}

