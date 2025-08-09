import * as PostalMime from 'postal-mime'
import * as mimeDb from 'mime-db'
import * as unzipit from 'unzipit'
import * as pako from 'pako'
import { XMLParser } from 'fast-xml-parser'

import {
  Env,
  Email,
  Attachment,
  DmarcRecordRow,
  AlignmentType,
  DispositionType,
  DMARCResultType,
  PolicyOverrideType,
  UniCloudFunctionResponse,
  Address,
  UniCloudFunctionPayload,
} from './types'

export default {
  async email(message: any, env: Env, ctx: any): Promise<void> {
    console.log('🚀 ===== DMARC Email Worker Started =====')
    console.log('📧 Received email message at:', new Date().toISOString())
    console.log('📨 Message from:', message.from)
    console.log('📬 Message to:', message.to)
    console.log('📝 Message subject:', message.headers.get('subject') || 'No subject')
    console.log('📏 Message size:', message.rawSize, 'bytes')

    try {
      await handleEmail(message, env, ctx)
      console.log('✅ ===== Email Processing Completed =====')
    } catch (error) {
      console.error('❌ ===== Email Processing Failed =====')
      console.error('💥 Error details:', error)

      // 记录详细的错误信息
      if (error instanceof Error) {
        console.error('📋 Error stack:', error.stack)
        console.error('📋 Error name:', error.name)
        console.error('📋 Error message:', error.message)
      }

      // 记录消息上下文
      console.error('📧 Message context for debugging:')
      console.error('  - Message type:', typeof message)
      console.error('  - Message keys:', message ? Object.keys(message) : 'null')
      console.error('  - Has raw:', !!message?.raw)
      console.error('  - Raw type:', message?.raw ? typeof message.raw : 'N/A')
      console.error('  - Raw size:', message.rawSize)

      // 不要重新抛出错误，让Worker优雅地处理
      console.log('⚠️ Worker will continue running despite this error')
    }
  },
}

async function handleEmail(message: any, env: Env, ctx: any): Promise<void> {
  console.log('🔧 ===== Starting Email Processing =====')

  const parser = new PostalMime.default()
  console.log('📦 Initialized PostalMime parser')

  // 全局错误处理包装
  try {
    // 解析邮件内容
    console.log('📖 Step 1: Parsing email content...')
    console.log('📧 Raw message info:')
    console.log('  - Message type:', typeof message)
    console.log('  - Has raw property:', !!message.raw)
    
    if (!message.raw) {
      throw new Error('Message raw content is missing')
    }

    const arrayBuffer = await new Response(message.raw).arrayBuffer()
    console.log('📧 ArrayBuffer created, size:', arrayBuffer.byteLength, 'bytes')

    const email = await parser.parse(arrayBuffer) as Email
    console.log('✅ Email parsed successfully')

    // 安全地输出邮件详情，处理可能的编码问题
    console.log('📧 Email details:')
    try {
      const safeFrom = email.from?.address || 'unknown'
      const safeSubject = sanitizeString(email.subject || 'No subject')
      const safeDate = email.date || 'No date'
      const attachmentCount = email.attachments?.length || 0

      console.log(' - From:', safeFrom)
      console.log(' - Subject:', safeSubject)
      console.log(' - Date:', safeDate)
      console.log(' - Attachment count:', attachmentCount)
      console.log(' - Message ID:', email.messageId || 'No ID')
      console.log(' - Has HTML:', !!email.html)
      console.log(' - Has Text:', !!email.text)
    } catch (detailError) {
      console.warn('⚠️ Warning: Could not display email details due to encoding issues:', detailError)
      console.log(' - From: [encoding issue]')
      console.log(' - Subject: [encoding issue]')
      console.log(' - Date: [encoding issue]')
      console.log(' - Attachment count:', email.attachments?.length || 0)
    }

    // 额外的安全检查：确保email对象结构完整
    if (!email || typeof email !== 'object') {
      throw new Error('Invalid email object structure')
    }

    // 确保attachments属性存在
    if (!email.attachments) {
      console.log('ℹ️ Email attachments property is undefined, initializing as empty array')
      email.attachments = []
    }
    
    // 确保attachments是数组
    if (!Array.isArray(email.attachments)) {
      console.warn('⚠️ Email attachments is not an array, converting to empty array')
      email.attachments = []
    }

    // 处理附件（如果有的话）
    console.log('📎 Step 2: Processing attachments...')
    let attachment: Attachment | null = null
    let reportRows: DmarcRecordRow[] = []
    let emailType = 'regular' // 邮件类型：regular, dmarc_report, attachment_only

    if (email.attachments && email.attachments.length > 0) {
      console.log('📄 Found', email.attachments.length, 'attachment(s)')
      attachment = email.attachments[0]

      try {
        const safeFilename = sanitizeString(attachment.filename || 'unnamed')
        const safeMimeType = attachment.mimeType || 'unknown'
        const contentSize = typeof attachment.content === 'string' ? attachment.content.length :
          (attachment.content instanceof ArrayBuffer ? attachment.content.byteLength : 0)

        console.log('📄 Attachment details:')
        console.log('  - Filename:', safeFilename)
        console.log('  - MIME type:', safeMimeType)
        console.log('  - Size:', contentSize, 'bytes')
        console.log('  - Disposition:', attachment.disposition || 'unknown')
        console.log('  - Content type:', typeof attachment.content)
      } catch (attachmentDetailError) {
        console.warn('⚠️ Warning: Could not display attachment details due to encoding issues:', attachmentDetailError)
        console.log('📄 Attachment details: [encoding issues]')
      }

      // 尝试解析XML获取DMARC报告数据（如果是DMARC报告的话）
      console.log('🔍 Step 3: Attempting to parse attachment as DMARC report...')
      try {
        const reportJSON = await getDMARCReportXML(attachment)
        console.log('✅ Successfully parsed as DMARC report')

        try {
          const orgName = sanitizeString(reportJSON?.feedback?.report_metadata?.org_name || 'Unknown')
          const reportId = sanitizeString(reportJSON?.feedback?.report_metadata?.report_id || 'Unknown')
          const domain = sanitizeString(reportJSON?.feedback?.policy_published?.domain || 'Unknown')

          console.log('📊 Report metadata:')
          console.log('  - Organization name:', orgName)
          console.log('  - Report ID:', reportId)
          console.log('  - Domain:', domain)
        } catch (metadataError) {
          console.warn('⚠️ Warning: Could not display report metadata due to encoding issues:', metadataError)
          console.log('📊 Report metadata: [encoding issues]')
        }

        reportRows = getReportRows(reportJSON)
        console.log('📈 Extracted', reportRows.length, 'DMARC records from report')
        emailType = 'dmarc_report'
      } catch (parseError) {
        const err = parseError as Error
        console.log('ℹ️ Attachment is not a valid DMARC report, treating as regular email with attachment')
        console.log('📋 Parse error:', err.message)
        emailType = 'attachment_only'
        // 继续处理，只是没有DMARC数据
      }
    } else {
      // ***** 这是关键的修改点 *****
      // 没有附件是一个正常情况，不是错误。记录信息并继续。
      console.log('ℹ️ No attachments found, treating as regular email')
      emailType = 'regular'
      // 确保变量状态正确
      attachment = null
      reportRows = []
    }

    // 记录邮件类型和处理状态
    console.log('📋 Email classification:')
    console.log('  - Type:', emailType)
    console.log('  - Has attachment:', !!attachment)
    console.log('  - DMARC records found:', reportRows.length)
    console.log('  - Processing status: Ready to continue')

    // 调用UniCloud云函数处理数据（无论是否有附件都调用）
    console.log('☁️ Step 4: Calling UniCloud function to process email data...')
    try {
      await callUniCloudFunction(email, attachment, reportRows)
      console.log('✅ UniCloud function call completed successfully')
    } catch (cloudFunctionError) {
      console.error('❌ UniCloud function call failed:', cloudFunctionError)
      // 即使云函数调用失败，也不应该让整个邮件处理失败
      console.log('⚠️ Continuing with email processing despite cloud function failure')
    }

    // 根据邮件类型输出不同的成功信息
    if (emailType === 'dmarc_report') {
      console.log('🎉 DMARC report processing completed successfully!')
      console.log('📊 Processed', reportRows.length, 'DMARC records')
    } else if (emailType === 'attachment_only') {
      console.log('✅ Email with attachment processed successfully!')
      console.log('📎 Attachment processed (not a DMARC report)')
    } else {
      console.log('✅ Regular email processed successfully!')
      console.log('📧 No attachments, standard email processing completed')
    }
  } catch (error) {
    const err = error as Error
    console.error('❌ Email processing error:', error)
    console.error('📋 Error details:', {
      message: err.message,
      stack: err.stack,
      name: err.name
    })

    // 添加更多上下文信息
    if (message) {
      console.error('📧 Message context:')
      console.error('  - Message type:', typeof message)
      console.error('  - Has raw property:', !!message.raw)
    }

    // 不要重新抛出错误，让上层try...catch块处理
    throw error;
  }
}

// 安全字符串处理函数
function sanitizeString(input: string): string {
  if (!input) return 'unknown'
  
  try {
    let cleaned = input
      .replace(/[\u0000-\u001F\u007F-\u009F]/g, '') // 移除控制字符
      .replace(/[\uFFFD]/g, '?') // 替换替换字符
      .trim()
    
    if (!cleaned) return 'unknown'
    
    if (cleaned.length > 200) {
      cleaned = cleaned.substring(0, 200) + '...'
    }
    
    return cleaned
  } catch (error) {
    console.warn('⚠️ String sanitization failed:', error)
    return 'encoding_error'
  }
}

async function getDMARCReportXML(attachment: Attachment) {
  console.log('🔍 ===== Starting XML Parsing =====')
  console.log('📄 Attachment MIME type:', attachment.mimeType)

  let xml: string;
  const xmlParser = new XMLParser()
  const extension = mimeDb[attachment.mimeType]?.extensions?.[0] || ''
  console.log('📝 Detected file extension:', extension || 'Unknown')

  try {
    const content = attachment.content;
    
    // Helper to convert ArrayBuffer to Uint8Array
    const toUint8Array = (data: string | ArrayBuffer): Uint8Array => {
      if (data instanceof ArrayBuffer) {
        return new Uint8Array(data);
      }
      // This is a fallback and assumes string is latin1 encoded if it's not text.
      // For binary gzipped data, it should already be an ArrayBuffer.
      const encoder = new TextEncoder();
      return encoder.encode(data);
    };

    switch (extension) {
      case 'gz':
        console.log('🗜️ Processing GZ compressed file...')
        xml = pako.inflate(toUint8Array(content), { to: 'string' })
        console.log('✅ GZ file decompression successful')
        console.log('📏 Decompressed XML size:', xml.length, 'characters')
        break

      case 'zip':
        console.log('📦 Processing ZIP compressed file...')
        xml = await getXMLFromZip(content)
        console.log('✅ ZIP file extraction successful')
        console.log('📏 Extracted XML size:', xml.length, 'characters')
        break

      case 'xml':
        console.log('📄 Processing pure XML file...')
        xml = (content instanceof ArrayBuffer) ? new TextDecoder().decode(content) : content as string;
        console.log('✅ XML file read successful')
        console.log('📏 XML size:', xml.length, 'characters')
        break

      default:
        // Fallback for mislabeled MIME types
        if(attachment.filename && attachment.filename.toLowerCase().endsWith('.xml')) {
            console.log('📝 Fallback to filename extension: detected .xml');
            xml = (content instanceof ArrayBuffer) ? new TextDecoder().decode(content) : content as string;
            break;
        }
        if(attachment.filename && attachment.filename.toLowerCase().endsWith('.zip')) {
            console.log('📝 Fallback to filename extension: detected .zip');
            xml = await getXMLFromZip(content)
            break;
        }
        if(attachment.filename && attachment.filename.toLowerCase().endsWith('.gz')) {
            console.log('📝 Fallback to filename extension: detected .gz');
            xml = pako.inflate(toUint8Array(content), { to: 'string' })
            break;
        }

        console.error('❌ Unknown file extension:', extension)
        console.error('📋 MIME type:', attachment.mimeType)
        throw new Error(`Unsupported attachment type for DMARC report: ${attachment.mimeType} (filename: ${attachment.filename})`)
    }

    console.log('🔄 Parsing XML content...')
    const parsedXML = xmlParser.parse(xml)
    console.log('✅ XML parsing successful')

    return parsedXML
  } catch (error) {
    const err = error as Error
    console.error('❌ XML parsing error:', err.message)
    console.error('📋 Error details:', {
      extension: extension,
      mimeType: attachment.mimeType,
      contentType: typeof attachment.content,
      contentSize: typeof attachment.content === 'string' ? attachment.content.length : 
        (attachment.content instanceof ArrayBuffer ? attachment.content.byteLength : 'Unknown')
    })
    throw error
  }
}

async function getXMLFromZip(content: string | ArrayBuffer): Promise<string> {
  console.log('📦 ===== Extracting ZIP file =====')

  try {
    // Ensure content is ArrayBuffer for unzipit
    const buffer = content instanceof ArrayBuffer ? content : new TextEncoder().encode(content).buffer;
    
    console.log('🔄 Decompressing content...')
    const { entries } = await unzipit.unzip(buffer)
    const entryNames = Object.keys(entries);
    console.log('📁 Found ZIP entries:', entryNames);

    if (entryNames.length === 0) {
      console.error('❌ No entries found in ZIP file')
      throw new Error('ZIP file is empty')
    }

    // Find the first .xml file, case-insensitive
    const xmlEntryName = entryNames.find(name => name.toLowerCase().endsWith('.xml'));
    if (!xmlEntryName) {
        throw new Error('No .xml file found in ZIP archive.');
    }
    const xmlEntry = entries[xmlEntryName];

    console.log(`📖 Reading content of the first XML entry found: ${xmlEntry.name}`);
    const xmlContent = await xmlEntry.text()
    console.log('✅ ZIP entry extraction successful')
    console.log('📏 Extracted content size:', xmlContent.length, 'characters')

    return xmlContent
  } catch (error) {
    const err = error as Error
    console.error('❌ Error extracting ZIP file:', error)
    console.error('📋 Error details:', {
      message: err.message,
      contentType: typeof content,
      contentSize: content instanceof ArrayBuffer ? content.byteLength : (content as string).length
    })
    throw error
  }
}

function getReportRows(report: any): DmarcRecordRow[] {
  console.log('📊 ===== Processing DMARC report data =====')

  try {
    console.log('🔍 Validating report structure...')
    const reportMetadata = report?.feedback?.report_metadata
    const policyPublished = report?.feedback?.policy_published
    const recordsSource = report?.feedback?.record

    console.log('📋 Report validation:')
    console.log('  - Has feedback data:', !!report.feedback)
    console.log('  - Has metadata:', !!reportMetadata)
    console.log('  - Has policy:', !!policyPublished)
    console.log('  - Has records data:', !!recordsSource)

    if (!report?.feedback || !reportMetadata || !policyPublished || !recordsSource) {
      console.error('❌ Invalid XML structure or missing key components.')
      throw new Error('Invalid DMARC XML structure')
    }
    
    const records = Array.isArray(recordsSource) ? recordsSource : [recordsSource];

    console.log('📊 Report metadata:')
    console.log('  - Report ID:', reportMetadata.report_id)
    console.log('  - Organization:', reportMetadata.org_name)
    console.log('  - Date range:', reportMetadata.date_range?.begin, 'to', reportMetadata.date_range?.end)

    console.log('📈 Processing', records.length, 'records...')
    const listEvents: DmarcRecordRow[] = []

    for (let index = 0; index < records.length; index++) {
      const record = records[index]
      if (!record || !record.row || !record.identifiers || !record.row.policy_evaluated) {
          console.warn(`⚠️ Skipping invalid record at index ${index}. Missing required fields.`);
          continue;
      }
      console.log(`🔄 Processing record ${index + 1}/${records.length}`)
      console.log('  - Source IP address:', record.row?.source_ip)
      console.log('  - Count:', record.row?.count)

      const reportRow: DmarcRecordRow = {
        reportMetadataReportId: reportMetadata.report_id?.toString().replace(/-/g, '_') || '',
        reportMetadataOrgName: reportMetadata.org_name || '',
        reportMetadataDateRangeBegin: parseInt(reportMetadata.date_range?.begin) || 0,
        reportMetadataDateRangeEnd: parseInt(reportMetadata.date_range?.end) || 0,
        reportMetadataError: reportMetadata.error ? JSON.stringify(reportMetadata.error) : '',

        policyPublishedDomain: policyPublished.domain || '',
        policyPublishedADKIM: AlignmentType[policyPublished.adkim as keyof typeof AlignmentType] ?? AlignmentType.r,
        policyPublishedASPF: AlignmentType[policyPublished.aspf as keyof typeof AlignmentType] ?? AlignmentType.r,
        policyPublishedP: DispositionType[policyPublished.p as keyof typeof DispositionType] ?? DispositionType.none,
        policyPublishedSP: DispositionType[policyPublished.sp as keyof typeof DispositionType] ?? DispositionType.none,
        policyPublishedPct: parseInt(policyPublished.pct) || 100,

        recordRowSourceIP: record.row?.source_ip || '',
        recordRowCount: parseInt(record.row?.count) || 0,
        recordRowPolicyEvaluatedDKIM: DMARCResultType[record.row?.policy_evaluated?.dkim as keyof typeof DMARCResultType] ?? DMARCResultType.fail,
        recordRowPolicyEvaluatedSPF: DMARCResultType[record.row?.policy_evaluated?.spf as keyof typeof DMARCResultType] ?? DMARCResultType.fail,
        recordRowPolicyEvaluatedDisposition:
          DispositionType[record.row?.policy_evaluated?.disposition as keyof typeof DispositionType] ?? DispositionType.none,

        recordRowPolicyEvaluatedReasonType:
          PolicyOverrideType[record.row?.policy_evaluated?.reason?.type as keyof typeof PolicyOverrideType] ?? PolicyOverrideType.other,
        recordIdentifiersEnvelopeTo: record.identifiers?.envelope_to || '',
        recordIdentifiersHeaderFrom: record.identifiers?.header_from || '',
      }

      listEvents.push(reportRow)
      console.log(`✅ Record ${index + 1} processed successfully`)
    }

    console.log('🎉 All records processed successfully!')
    console.log('📊 Total records created:', listEvents.length)
    return listEvents
  } catch (error) {
    const err = error as Error
    console.error('❌ Error in getReportRows function:', error)
    console.error('📋 Error details:', {
      message: err.message,
    })
    throw error
  }
}

// 调用UniCloud云函数处理邮件数据
async function callUniCloudFunction(
  email: Email,
  attachment: Attachment | null,
  reportRows: DmarcRecordRow[]
): Promise<void> {
  console.log('☁️ ===== Calling UniCloud Function =====')
  
  // 详细记录输入数据状态
  console.log('📊 Input data summary:')
  console.log('  - Email sender:', email.from?.address || 'undefined')
  console.log('  - DMARC records:', reportRows.length, 'records')
  console.log('  - Email type:', determineEmailType(attachment, reportRows))

  const cloudFunctionUrl = 'https://env-00jxt0xsffn5.dev-hz.cloudbasefunction.cn/POST_cloudflare_edukg_email'

  try {
    // 准备发送给云函数的数据
    console.log('📦 Preparing payload...')
    const payload = preparePayload(email, attachment, reportRows)
    
    console.log('📦 Payload summary:')
    console.log('  - Email sender:', payload.emailInfo.from)
    console.log('  - Email subject:', payload.emailInfo.subject)
    console.log('  - Has attachment:', !!payload.attachment)
    if (payload.attachment) {
      console.log('  - Attachment filename:', payload.attachment.filename)
      console.log('  - Attachment size:', payload.attachment.size, 'bytes')
    }
    console.log('  - DMARC records count:', payload.dmarcRecords.length)
    
    // 检查payload大小，避免过大的请求
    const payloadSize = JSON.stringify(payload).length;
    console.log('  - Payload size:', payloadSize, 'characters');
    if (payloadSize > 10 * 1024 * 1024) { // 10MB限制
      console.warn('⚠️ Payload size is large:', Math.round(payloadSize / 1024 / 1024 * 100) / 100, 'MB')
    }

    console.log('🚀 Sending request to UniCloud function...')
    console.log('🌐 Function URL:', cloudFunctionUrl)

    // 设置请求超时
    const controller = new AbortController()
    const timeoutId = setTimeout(() => controller.abort(), 30000) // 30秒超时

    try {
      console.log('📡 Making fetch request...')
      const response = await fetch(cloudFunctionUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'User-Agent': 'Cloudflare-Workers-DMARC-Processor/1.2.0',
        },
        body: JSON.stringify(payload),
        signal: controller.signal
      })

      clearTimeout(timeoutId)

      console.log('📡 Response status:', response.status, response.statusText)
      
      const headers: Record<string, string> = {}
      response.headers.forEach((value, key) => {
        headers[key] = value
      })
      console.log('📋 Response headers:', headers)

      if (response.ok) {
        console.log('📄 Reading response body...')
        const result = await response.json() as UniCloudFunctionResponse
        console.log('✅ UniCloud function executed successfully!')
        console.log('📄 Response data:', JSON.stringify(result, null, 2))

        if (result.success) {
          console.log('🎉 Data processing completed successfully!')
          if(result.message) console.log('💬 Success message:', result.message)
        } else {
          console.warn('⚠️ Function executed but reported an error:', result.error || 'Unknown error')
        }
      } else {
        console.log('📄 Reading error response body...')
        const errorText = await response.text()
        console.error('❌ UniCloud function call failed!')
        const errorMessage = getDetailedErrorMessage(response.status, errorText)
        throw new Error(errorMessage)
      }
    } catch (fetchError: any) {
      clearTimeout(timeoutId)
      
      if (fetchError.name === 'AbortError') {
        console.error('⏰ Request timeout after 30 seconds')
        throw new Error('Request timeout after 30 seconds')
      }
      console.error('📡 Fetch error:', fetchError)
      throw fetchError
    }
  } catch (error) {
    const err = error as Error
    console.error('❌ Error calling UniCloud function:', err.message)
    
    if (shouldRetry(error)) {
      console.log('🔄 Retrying UniCloud function call...')
      try {
        await retryUniCloudCall(email, attachment, reportRows, cloudFunctionUrl)
        return
      } catch (retryError) {
        console.error('❌ Retry attempt failed:', retryError)
      }
    }
    
    // 向上抛出，让最外层catch处理
    throw error
  }
}

// 辅助函数：确定邮件类型
function determineEmailType(attachment: Attachment | null, reportRows: DmarcRecordRow[]): string {
  if (attachment && reportRows.length > 0) {
    return 'dmarc_report'
  } else if (attachment) {
    return 'attachment_only'
  } else {
    return 'regular'
  }
}

// 辅助函数：准备payload数据
function preparePayload(email: Email, attachment: Attachment | null, reportRows: DmarcRecordRow[]): UniCloudFunctionPayload {
  console.log('📦 Starting payload preparation...')
  
  // ArrayBuffer to Base64 utility
  const toBase64 = (buffer: ArrayBuffer): string => {
    let binary = '';
    const bytes = new Uint8Array(buffer);
    for (let i = 0; i < bytes.byteLength; i++) {
      binary += String.fromCharCode(bytes[i]);
    }
    return btoa(binary);
  };
  
  let attachmentPayload: UniCloudFunctionPayload['attachment'] | null = null;
  if (attachment) {
      let contentBase64 = '';
      if (attachment.content instanceof ArrayBuffer) {
          contentBase64 = toBase64(attachment.content);
      } else if (typeof attachment.content === 'string') {
          // Assuming the string is binary-like, btoa should work.
          // For UTF-8 strings, a more complex conversion is needed, but for mail attachments this is usually fine.
          contentBase64 = btoa(unescape(encodeURIComponent(attachment.content)));
      }

      attachmentPayload = {
          filename: sanitizeString(attachment.filename || 'unnamed'),
          mimeType: attachment.mimeType || 'application/octet-stream',
          content: contentBase64,
          size: contentBase64.length, // Base64 size, not raw size
      };
  }
  
  const payload: UniCloudFunctionPayload = {
    emailInfo: {
      from: email.from?.address || 'unknown',
      to: Array.isArray(email.to) ? email.to.map((addr: Address) => addr?.address || 'unknown').filter(Boolean) : [],
      subject: sanitizeString(email.subject || 'No subject'),
      date: email.date || new Date().toISOString(),
      messageId: email.messageId || 'unknown',
    },
    attachment: attachmentPayload!, // The type expects attachment, but it can be null. We'll handle this in the cloud function. Let's send null instead.
    dmarcRecords: reportRows,
    processedAt: new Date().toISOString(),
    workerInfo: {
      version: '1.2.0',
      source: 'cloudflare-workers',
    },
  };

  // Correcting the payload structure if there's no attachment
  if (!attachmentPayload) {
      // @ts-ignore - We are intentionally sending a payload that might differ slightly for non-attachment cases
      // The receiving cloud function must be robust enough to handle a null or missing attachment field.
      delete payload.attachment;
  }
  
  console.log('📦 Payload prepared successfully')
  return payload
}

// 辅助函数：获取详细的错误信息
function getDetailedErrorMessage(status: number, errorText: string): string {
  switch (status) {
    case 400: return `Bad Request (400): Invalid data format - ${errorText}`
    case 401: return `Unauthorized (401): Authentication required - ${errorText}`
    case 403: return `Forbidden (403): Access denied - ${errorText}`
    case 404: return `Not Found (404): UniCloud function not found - ${errorText}`
    case 413: return `Payload Too Large (413): Request body too large - ${errorText}`
    case 429: return `Too Many Requests (429): Rate limit exceeded - ${errorText}`
    case 500: return `Internal Server Error (500): UniCloud function error - ${errorText}`
    case 502: return `Bad Gateway (502): UniCloud service unavailable - ${errorText}`
    case 503: return `Service Unavailable (503): UniCloud service temporarily unavailable - ${errorText}`
    case 504: return `Gateway Timeout (504): UniCloud function timeout - ${errorText}`
    default: return `HTTP Error ${status}: ${errorText}`
  }
}

// 辅助函数：判断是否应该重试
function shouldRetry(error: any): boolean {
  if (!(error instanceof Error)) return false;
  const errorMessage = error.message.toLowerCase();
  const retryableErrors = [
    'timeout',
    'network',
    'connection',
    '502',
    '503',
    '504'
  ];
  
  return retryableErrors.some(retryableError => 
    errorMessage.includes(retryableError)
  );
}

// 辅助函数：重试UniCloud调用
async function retryUniCloudCall(
  email: Email,
  attachment: Attachment | null,
  reportRows: DmarcRecordRow[],
  cloudFunctionUrl: string
): Promise<void> {
  console.log('🔄 Attempting retry with simplified payload...')
  
  // 在重试时，我们只发送最关键的信息，不发送附件内容，以增加成功率
  const simplifiedPayload = {
    emailInfo: {
      from: email.from?.address || 'unknown',
      to: email.to?.map((addr: Address) => addr.address).filter(Boolean) || [],
      subject: `[RETRY] ${sanitizeString(email.subject || 'No subject')}`,
      date: email.date || new Date().toISOString(),
      messageId: email.messageId || 'unknown'
    },
    attachment: attachment ? {
      filename: attachment.filename || 'unnamed',
      mimeType: attachment.mimeType || 'application/octet-stream',
      size: typeof attachment.content === 'string' ? attachment.content.length : 
        (attachment.content instanceof ArrayBuffer ? attachment.content.byteLength : 0),
      content: null // 不在重试时发送内容
    } : null,
    dmarcRecords: reportRows,
    processedAt: new Date().toISOString(),
    workerInfo: {
      version: '1.2.0-retry',
      source: 'cloudflare-workers',
      isRetry: true
    }
  }
  
  console.log('📦 Simplified payload prepared for retry')
  
  try {
    console.log('📡 Making retry request...')
    const response = await fetch(cloudFunctionUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'User-Agent': 'Cloudflare-Workers-DMARC-Processor/1.2.0-Retry',
        'X-Is-Retry': 'true',
      },
      body: JSON.stringify(simplifiedPayload)
    })
    
    console.log('📡 Retry response status:', response.status, response.statusText)
    
    if (!response.ok) {
      const errorText = await response.text()
      console.error('❌ Retry failed with status:', response.status)
      console.error('📋 Retry error response:', errorText)
      throw new Error(`Retry failed: ${response.status} ${response.statusText} - ${errorText}`)
    }
    
    console.log('✅ Retry attempt successful!')
    const result = await response.json() as UniCloudFunctionResponse;
    console.log('📄 Retry response data:', JSON.stringify(result, null, 2))
  } catch (retryError) {
    console.error('❌ Retry request failed:', retryError)
    throw retryError
  }
}