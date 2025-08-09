export interface Env {
  // 不需要任何环境变量，直接调用UniCloud云函数
}

export type Header = Record<string, string>

export type Address = {
  address: string
  name: string
}

export type Attachment = {
  filename: string
  mimeType: string
  disposition: 'attachment' | 'inline' | null
  related?: boolean
  contentId?: string
  content: string | ArrayBuffer
}

export type Email = {
  headers: Header[]
  from: Address
  sender?: Address
  replyTo?: Address[]
  deliveredTo?: string
  returnPath?: string
  to: Address[]
  cc?: Address[]
  bcc?: Address[]
  subject?: string
  messageId: string
  inReplyTo?: string
  references?: string
  date?: string
  html?: string
  text?: string
  attachments: Attachment[]
}

export type DmarcRecordRow = {
  reportMetadataReportId: string
  reportMetadataOrgName: string
  reportMetadataDateRangeBegin: number
  reportMetadataDateRangeEnd: number
  reportMetadataError: string

  policyPublishedDomain: string
  policyPublishedADKIM: AlignmentType
  policyPublishedASPF: AlignmentType
  policyPublishedP: DispositionType
  policyPublishedSP: DispositionType
  policyPublishedPct: number

  recordRowSourceIP: string
  recordRowCount: number
  recordRowPolicyEvaluatedDKIM: DMARCResultType
  recordRowPolicyEvaluatedSPF: DMARCResultType
  recordRowPolicyEvaluatedDisposition: DispositionType
  recordRowPolicyEvaluatedReasonType: PolicyOverrideType
  recordIdentifiersEnvelopeTo: string
  recordIdentifiersHeaderFrom: string
}

// UniCloud云函数请求数据类型
export type UniCloudFunctionPayload = {
  emailInfo: {
    from: string
    to: string[]
    subject: string
    date: string
    messageId: string
  }
  attachment: {
    filename: string
    mimeType: string
    content: string | ArrayBuffer
    size: number
  }
  dmarcRecords: DmarcRecordRow[]
  processedAt: string
  workerInfo: {
    version: string
    source: string
  }
}

// UniCloud云函数响应数据类型
export type UniCloudFunctionResponse = {
  success: boolean
  message?: string
  error?: string
  uploadedFileUrl?: string
  insertedRecords?: number
  processingTime?: number
  data?: any
}

export enum AlignmentType {
  r = 0,
  s = 1,
}

export enum DMARCResultType {
  fail = 0,
  pass = 1,
}

export enum DispositionType {
  none = 0,
  quarantine = 1,
  reject = 2,
}

export enum PolicyOverrideType {
  other = 0,
  forwarded = 1,
  sampled_out = 2,
  trusted_forwarder = 3,
  mailing_list = 4,
  local_policy = 5,
}