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
} from './types'

// 移除UniCloudClient导入，改为直接HTTP调用

export default {
  async email(message: any, env: Env, ctx: any): Promise<void> {
    console.log('🚀 ===== DMARC Email Worker Started =====')
    console.log('📧 Received email message at:', new Date().toISOString())
    console.log('📨 Message from:', message.from)
    console.log('📬 Message to:', message.to)
    console.log('📝 Message subject:', message.headers.get('subject') || 'No subject')
    console.log('📏 Message size:', message.raw?.length || 'unknown', 'bytes')

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
    console.log('  - Raw content type:', message.raw ? typeof message.raw : 'N/A')

    if (!message.raw) {
      throw new Error('Message raw content is missing')
    }

    const rawEmail = new Response(message.raw)
    console.log('📧 Response created from raw message')

    const arrayBuffer = await rawEmail.arrayBuffer()
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
      console.log(' - Raw size:', arrayBuffer.byteLength, 'bytes')
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
      console.log('ℹ️ Email attachments is not an array, converting to empty array')
      email.attachments = []
    }

    // 处理附件（如果有的话）
    console.log('📎 Step 2: Processing attachments...')
    let attachment = null
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

        if (contentSize === 0 || contentSize === null || contentSize === undefined) {
          console.warn('⚠️ Warning: Attachment content size is invalid:', contentSize)
        }
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
        console.log('📋 Parse error stack:', err.stack)
        emailType = 'attachment_only'
        // 继续处理，只是没有DMARC数据
      }
    } else {
      console.log('ℹ️ No attachments found, treating as regular email')
      console.log('📧 This is a standard email without attachments - processing normally')
      console.log('📋 Regular email processing will continue with basic email data')
      emailType = 'regular'
      // 确保没有附件时设置默认值，继续正常流程
      attachment = null
      reportRows = []
      console.log('✅ Regular email setup completed - ready for cloud function processing')
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

    console.log('🎯 ===== Email Processing Completed Successfully =====')

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
      console.error('  - Raw content type:', message.raw ? typeof message.raw : 'N/A')
    }

    // 记录详细的错误信息用于调试
    console.error('🔍 Detailed error analysis:')
    console.error('  - Error type:', err.constructor.name)
    console.error('  - Error message:', err.message)
    console.error('  - Error stack:', err.stack)

    // 不要重新抛出错误，让Worker优雅地处理
    console.log('⚠️ Worker will continue running despite this error')
    console.log('📧 Email processing failed but Worker remains stable')
  }
}

// 新增：安全字符串处理函数
function sanitizeString(input: string): string {
  if (!input) return 'unknown'

  try {
    // 尝试清理可能导致问题的字符
    let cleaned = input
      .replace(/[\u0000-\u001F\u007F-\u009F]/g, '') // 移除控制字符
      .replace(/[\uFFFD]/g, '?') // 替换替换字符
      .trim()

    // 如果清理后为空，返回默认值
    if (!cleaned) return 'unknown'

    // 限制长度避免日志过长
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

  let xml
  const xmlParser = new XMLParser()
  const extension = mimeDb[attachment.mimeType]?.extensions?.[0] || ''
  console.log('📝 Detected file extension:', extension || 'Unknown')

  try {
    switch (extension) {
      case 'gz':
        console.log('🗜️ Processing GZ compressed file...')
        xml = pako.inflate(new TextEncoder().encode(attachment.content as string), { to: 'string' })
        console.log('✅ GZ file decompression successful')
        console.log('📏 Decompressed XML size:', xml.length, 'characters')
        break

      case 'zip':
        console.log('📦 Processing ZIP compressed file...')
        xml = await getXMLFromZip(attachment.content)
        console.log('✅ ZIP file extraction successful')
        console.log('📏 Extracted XML size:', xml.length, 'characters')
        break

      case 'xml':
        console.log('📄 Processing pure XML file...')
        xml = await new Response(attachment.content).text()
        console.log('✅ XML file read successful')
        console.log('📏 XML size:', xml.length, 'characters')
        break

      default:
        console.error('❌ Unknown file extension:', extension)
        console.error('📋 MIME type:', attachment.mimeType)
        throw new Error(`Unknown extension: ${extension}`)
    }

    console.log('🔄 Parsing XML content...')
    const parsedXML = await xmlParser.parse(xml)
    console.log('✅ XML parsing successful')
    console.log('📊 XML structure preview:', JSON.stringify(parsedXML, null, 2).substring(0, 500) + '...')

    return parsedXML
  } catch (error) {
    const err = error as Error
    console.error('❌ XML parsing error:', error)
    console.error('📋 Error details:', {
      message: err.message,
      extension: extension,
      mimeType: attachment.mimeType,
      contentType: typeof attachment.content,
      contentSize: typeof attachment.content === 'string' ? attachment.content.length :
        (attachment.content instanceof ArrayBuffer ? attachment.content.byteLength : 'Unknown')
    })
    throw error
  }
}

async function getXMLFromZip(content: string | ArrayBuffer | Blob | unzipit.TypedArray | unzipit.Reader) {
  console.log('📦 ===== Extracting ZIP file =====')

  try {
    console.log('🔄 Decompressing content...')
    const { entries } = await unzipit.unzipRaw(content)
    console.log('📁 Found ZIP entries:', entries.length, 'entries')

    if (entries.length === 0) {
      console.error('❌ No entries found in ZIP file')
      throw new Error('ZIP file is empty')
    }

    // List all entries
    entries.forEach((entry, index) => {
      console.log(`📄 Entry ${index + 1}:`, entry.name, `(${entry.size} bytes)`)
    })

    console.log('📖 Reading content of the first entry...')
    const xmlContent = await entries[0].text()
    console.log('✅ ZIP entry extraction successful')
    console.log('📏 Extracted content size:', xmlContent.length, 'characters')

    return xmlContent
  } catch (error) {
    const err = error as Error
    console.error('❌ Error extracting ZIP file:', error)
    console.error('📋 Error details:', {
      message: err.message,
      contentType: typeof content,
      contentSize: content instanceof ArrayBuffer ? content.byteLength : 'Unknown'
    })
    throw error
  }
}

function getReportRows(report: any): DmarcRecordRow[] {
  console.log('📊 ===== Processing DMARC report data =====')

  try {
    console.log('🔍 Validating report structure...')
    const reportMetadata = report.feedback?.report_metadata
    const policyPublished = report.feedback?.policy_published
    const records = Array.isArray(report.feedback?.record) ? report.feedback.record : [report.feedback?.record]

    console.log('📋 Report validation:')
    console.log('  - Has feedback data:', !!report.feedback)
    console.log('  - Has metadata:', !!reportMetadata)
    console.log('  - Has policy:', !!policyPublished)
    console.log('  - Has records:', !!records && records.length > 0)

    if (!report.feedback || !reportMetadata || !policyPublished || !records) {
      console.error('❌ Invalid XML structure')
      console.error('📋 Missing components:', {
        feedback: !report.feedback,
        metadata: !reportMetadata,
        policy: !policyPublished,
        records: !records
      })
      throw new Error('Invalid XML')
    }

    console.log('📊 Report metadata:')
    console.log('  - Report ID:', reportMetadata.report_id)
    console.log('  - Organization:', reportMetadata.org_name)
    console.log('  - Date range:', reportMetadata.date_range?.begin, 'to', reportMetadata.date_range?.end)

    console.log('🛡️ Published policy:')
    console.log('  - Domain:', policyPublished.domain)
    console.log('  - Policy:', policyPublished.p)
    console.log('  - Percentage:', policyPublished.pct)
    console.log('  - DKIM alignment:', policyPublished.adkim)
    console.log('  - SPF alignment:', policyPublished.aspf)

    console.log('📈 Processing', records.length, 'records...')
    const listEvents: DmarcRecordRow[] = []

    for (let index = 0; index < records.length; index++) {
      const record = records[index]
      console.log(`🔄 Processing record ${index + 1}/${records.length}`)
      console.log('  - Source IP address:', record.row?.source_ip)
      console.log('  - Count:', record.row?.count)
      console.log('  - DKIM result:', record.row?.policy_evaluated?.dkim)
      console.log('  - SPF result:', record.row?.policy_evaluated?.spf)
      console.log('  - Disposition:', record.row?.policy_evaluated?.disposition)

      const reportRow: DmarcRecordRow = {
        reportMetadataReportId: reportMetadata.report_id?.toString().replace('-', '_') || '',
        reportMetadataOrgName: reportMetadata.org_name || '',
        reportMetadataDateRangeBegin: parseInt(reportMetadata.date_range?.begin) || 0,
        reportMetadataDateRangeEnd: parseInt(reportMetadata.date_range?.end) || 0,
        reportMetadataError: JSON.stringify(reportMetadata.error) || '',

        policyPublishedDomain: policyPublished.domain || '',
        policyPublishedADKIM: AlignmentType[policyPublished.adkim as keyof typeof AlignmentType] || 0,
        policyPublishedASPF: AlignmentType[policyPublished.aspf as keyof typeof AlignmentType] || 0,
        policyPublishedP: DispositionType[policyPublished.p as keyof typeof DispositionType] || 0,
        policyPublishedSP: DispositionType[policyPublished.sp as keyof typeof DispositionType] || 0,
        policyPublishedPct: parseInt(policyPublished.pct) || 0,

        recordRowSourceIP: record.row?.source_ip || '',
        recordRowCount: parseInt(record.row?.count) || 0,
        recordRowPolicyEvaluatedDKIM: DMARCResultType[record.row?.policy_evaluated?.dkim as keyof typeof DMARCResultType] || 0,
        recordRowPolicyEvaluatedSPF: DMARCResultType[record.row?.policy_evaluated?.spf as keyof typeof DMARCResultType] || 0,
        recordRowPolicyEvaluatedDisposition:
          DispositionType[record.row?.policy_evaluated?.disposition as keyof typeof DispositionType] || 0,

        recordRowPolicyEvaluatedReasonType:
          PolicyOverrideType[record.row?.policy_evaluated?.reason?.type as keyof typeof PolicyOverrideType] || 0,
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
      reportStructure: JSON.stringify(report, null, 2).substring(0, 1000) + '...'
    })
    throw error
  }
}

// 调用UniCloud云函数处理邮件数据
async function callUniCloudFunction(
  email: any,
  attachment: Attachment | null,
  reportRows: DmarcRecordRow[]
): Promise<void> {
  console.log('☁️ ===== Calling UniCloud Function =====')

  // 详细记录输入数据状态
  console.log('📊 Input data summary:')
  console.log('  - Email object:', !!email ? 'Valid' : 'Invalid')
  if (email) {
    console.log('  - Email from:', email.from?.address || 'undefined')
    console.log('  - Email to:', email.to?.map((addr: Address) => addr?.address || 'undefined'))
    console.log('  - Email subject:', email.subject || 'undefined')
    console.log('  - Email date:', email.date || 'undefined')
    console.log('  - Email messageId:', email.messageId || 'undefined')
    console.log('  - Email hasHtml:', !!email.html)
    console.log('  - Email hasText:', !!email.text)
    console.log('  - Email raw size:', email.raw ? 'present' : 'missing')
  }
  console.log('  - Attachment:', attachment ? `Present (${attachment.filename})` : 'None')
  if (attachment) {
    console.log('    - Filename:', attachment.filename || 'undefined')
    console.log('    - MIME type:', attachment.mimeType || 'undefined')
    console.log('    - Content type:', typeof attachment.content)
    console.log('    - Content size:', attachment.content ?
      (typeof attachment.content === 'string' ? attachment.content.length :
        attachment.content instanceof ArrayBuffer ? attachment.content.byteLength : 'unknown') : 'null')
  }
  console.log('  - DMARC records:', reportRows.length, 'records')
  console.log('  - Email type:', determineEmailType(attachment, reportRows))

  // 验证输入数据的完整性
  if (!email) {
    console.error('❌ Invalid email object provided')
    throw new Error('Invalid email object')
  }

  // 验证邮件基本信息的完整性
  console.log('🔍 Validating email data...')
  const emailValidation = validateEmailData(email)
  if (!emailValidation.isValid) {
    console.warn('⚠️ Email data validation warnings:', emailValidation.warnings)
    // 继续处理，但记录警告
  } else {
    console.log('✅ Email data validation passed')
  }

  const cloudFunctionUrl = 'https://env-00jxt0xsffn5.dev-hz.cloudbasefunction.cn/POST_cloudflare_edukg_email'

  try {
    // 准备发送给云函数的数据
    console.log('📦 Preparing payload...')
    const payload = preparePayload(email, attachment, reportRows)

    // 验证payload的完整性
    console.log('🔍 Validating payload...')
    const payloadValidation = validatePayload(payload)
    if (!payloadValidation.isValid) {
      console.warn('⚠️ Payload validation warnings:', payloadValidation.warnings)
      // 继续处理，但记录警告
    } else {
      console.log('✅ Payload validation passed')
    }

    console.log('📦 Payload summary:')
    console.log('  - Email sender:', payload.emailInfo.from)
    console.log('  - Email subject:', payload.emailInfo.subject)
    console.log('  - Has attachment:', !!payload.attachment)
    if (payload.attachment) {
      console.log('  - Attachment filename:', payload.attachment.filename)
      console.log('  - Attachment size:', payload.attachment.size, 'bytes')
    }
    console.log('  - DMARC records count:', payload.dmarcRecords.length)
    console.log('  - Payload size:', JSON.stringify(payload).length, 'characters')

    // 检查payload大小，避免过大的请求
    const payloadSize = JSON.stringify(payload).length
    if (payloadSize > 10 * 1024 * 1024) { // 10MB限制
      console.warn('⚠️ Payload size is large:', Math.round(payloadSize / 1024 / 1024 * 100) / 100, 'MB')
      // 可以考虑压缩或分块处理
    }

    console.log('🚀 Sending request to UniCloud function...')
    console.log('🌐 Function URL:', cloudFunctionUrl)

    // 设置请求超时和重试机制
    const controller = new AbortController()
    const timeoutId = setTimeout(() => controller.abort(), 30000) // 30秒超时

    try {
      console.log('📡 Making fetch request...')
      const response = await fetch(cloudFunctionUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'User-Agent': 'Cloudflare-Workers-DMARC-Processor/1.0',
          'X-Processing-Timestamp': new Date().toISOString(),
          'X-Record-Count': reportRows.length.toString(),
          'X-Has-Attachment': (!!attachment).toString()
        },
        body: JSON.stringify(payload),
        signal: controller.signal
      })

      clearTimeout(timeoutId)

      console.log('📡 Response status:', response.status, response.statusText)

      // 使用兼容的方式获取响应头
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

        // 记录处理结果
        if (result.success) {
          console.log('🎉 Data processing completed successfully!')
          if (result.uploadedFileUrl) {
            console.log('📁 File uploaded to:', result.uploadedFileUrl)
          }
          if (result.insertedRecords !== undefined) {
            console.log('💾 Database records inserted:', result.insertedRecords)
          }
          if (result.processingTime) {
            console.log('⏱️ Processing time:', result.processingTime, 'milliseconds')
          }
          if (result.message) {
            console.log('💬 Success message:', result.message)
          }
        } else {
          console.warn('⚠️ Function executed but reported an error:', result.error || 'Unknown error')
          // 即使有错误，也不抛出异常，因为函数本身执行成功了
        }
      } else {
        console.log('📄 Reading error response body...')
        const errorText = await response.text()
        console.error('❌ UniCloud function call failed!')
        console.error('📋 Error response:', errorText)

        // 根据HTTP状态码提供更详细的错误信息
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
    console.error('❌ Error calling UniCloud function:', error)
    console.error('📋 Error details:', {
      message: err.message,
      stack: err.stack,
      functionUrl: cloudFunctionUrl,
      recordCount: reportRows.length,
      hasAttachment: !!attachment,
      emailSubject: email.subject || 'No subject'
    })

    // 根据错误类型决定是否重试
    if (shouldRetry(error)) {
      console.log('🔄 Retrying UniCloud function call...')
      try {
        await retryUniCloudCall(email, attachment, reportRows, cloudFunctionUrl)
        return
      } catch (retryError) {
        console.error('❌ Retry attempt failed:', retryError)
      }
    }

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

// 辅助函数：验证邮件数据
function validateEmailData(email: any): { isValid: boolean; warnings: string[] } {
  const warnings: string[] = []

  if (!email.from?.address) {
    warnings.push('Missing sender email address')
  }

  if (!email.to || email.to.length === 0) {
    warnings.push('Missing recipient email addresses')
  }

  if (!email.subject) {
    warnings.push('Missing email subject')
  }

  if (!email.date) {
    warnings.push('Missing email date')
  }

  return {
    isValid: warnings.length === 0,
    warnings
  }
}

// 辅助函数：准备payload数据
function preparePayload(email: any, attachment: Attachment | null, reportRows: DmarcRecordRow[]): any {
  console.log('📦 Starting payload preparation...')

  // 安全地处理邮件内容，避免编码问题
  const safeSubject = sanitizeString(email.subject || 'No subject')
  const safeFrom = email.from?.address || 'unknown'
  const safeTo = Array.isArray(email.to) ? email.to.map((addr: Address) => addr?.address || 'unknown').filter((addr: string) => addr !== 'unknown') : ['unknown']

  // 验证关键数据
  if (!safeFrom || safeFrom === 'unknown') {
    console.warn('⚠️ Warning: Sender email is missing or invalid')
  }

  if (!safeTo || safeTo.length === 0 || safeTo.includes('unknown')) {
    console.warn('⚠️ Warning: Recipient emails are missing or invalid')
  }

  if (!safeSubject || safeSubject === 'No subject') {
    console.warn('⚠️ Warning: Email subject is missing or invalid')
  }

  // 处理附件信息
  let attachmentInfo = null
  if (attachment) {
    try {
      const contentSize = typeof attachment.content === 'string' ? attachment.content.length :
        (attachment.content instanceof ArrayBuffer ? attachment.content.byteLength : 0)

      attachmentInfo = {
        filename: sanitizeString(attachment.filename || 'unnamed'),
        mimeType: attachment.mimeType || 'application/octet-stream',
        content: attachment.content, // 原始内容，云函数会处理
        size: contentSize,
        disposition: attachment.disposition || 'attachment'
      }

      console.log('📎 Attachment info prepared:', {
        filename: attachmentInfo.filename,
        mimeType: attachmentInfo.mimeType,
        size: attachmentInfo.size,
        disposition: attachmentInfo.disposition
      })
    } catch (attachmentError) {
      console.warn('⚠️ Warning: Could not prepare attachment info:', attachmentError)
      attachmentInfo = null
    }
  }

  const payload = {
    // 邮件基本信息
    emailInfo: {
      from: safeFrom,
      to: safeTo,
      subject: safeSubject,
      date: email.date || new Date().toISOString(),
      messageId: email.messageId || 'unknown',
      hasHtml: !!email.html,
      hasText: !!email.text
    },

    // 附件信息（如果有的话）
    attachment: attachmentInfo,

    // 解析后的DMARC数据
    dmarcRecords: reportRows,

    // 处理时间戳
    processedAt: new Date().toISOString(),

    // Worker信息
    workerInfo: {
      version: '1.0.0',
      source: 'cloudflare-workers',
      processingTimestamp: new Date().toISOString()
    },

    // 处理统计信息
    processingStats: {
      totalRecords: reportRows.length,
      hasAttachment: !!attachment,
      emailType: determineEmailType(attachment, reportRows),
      processingDuration: Date.now() - new Date().getTime()
    }
  }

  console.log('📦 Payload prepared successfully')
  console.log('📊 Payload summary:')
  console.log('  - Email sender:', payload.emailInfo.from)
  console.log('  - Email recipients:', payload.emailInfo.to)
  console.log('  - Email subject:', payload.emailInfo.subject)
  console.log('  - Has attachment:', !!payload.attachment)
  console.log('  - DMARC records:', payload.dmarcRecords.length)

  return payload
}

// 辅助函数：验证payload数据
function validatePayload(payload: any): { isValid: boolean; warnings: string[] } {
  const warnings: string[] = []

  if (!payload.emailInfo.from || payload.emailInfo.from === 'unknown') {
    warnings.push('Sender email address is missing or invalid')
  }

  if (!payload.emailInfo.to || payload.emailInfo.to.length === 0 || payload.emailInfo.to.includes('unknown')) {
    warnings.push('Recipient email addresses are missing or invalid')
  }

  if (!payload.emailInfo.subject || payload.emailInfo.subject === 'No subject') {
    warnings.push('Email subject is missing or invalid')
  }

  if (payload.attachment && (!payload.attachment.filename || payload.attachment.size === 0)) {
    warnings.push('Attachment information is incomplete')
  }

  return {
    isValid: warnings.length === 0,
    warnings
  }
}

// 辅助函数：获取详细的错误信息
function getDetailedErrorMessage(status: number, errorText: string): string {
  switch (status) {
    case 400:
      return `Bad Request (400): Invalid data format - ${errorText}`
    case 401:
      return `Unauthorized (401): Authentication required - ${errorText}`
    case 403:
      return `Forbidden (403): Access denied - ${errorText}`
    case 404:
      return `Not Found (404): UniCloud function not found - ${errorText}`
    case 413:
      return `Payload Too Large (413): Request body too large - ${errorText}`
    case 429:
      return `Too Many Requests (429): Rate limit exceeded - ${errorText}`
    case 500:
      return `Internal Server Error (500): UniCloud function error - ${errorText}`
    case 502:
      return `Bad Gateway (502): UniCloud service unavailable - ${errorText}`
    case 503:
      return `Service Unavailable (503): UniCloud service temporarily unavailable - ${errorText}`
    case 504:
      return `Gateway Timeout (504): UniCloud function timeout - ${errorText}`
    default:
      return `HTTP Error ${status}: ${errorText}`
  }
}

// 辅助函数：判断是否应该重试
function shouldRetry(error: any): boolean {
  const errorMessage = error.message || ''
  const retryableErrors = [
    'timeout',
    'network',
    'connection',
    '502',
    '503',
    '504'
  ]

  return retryableErrors.some(retryableError =>
    errorMessage.toLowerCase().includes(retryableError)
  )
}

// 辅助函数：重试UniCloud调用
async function retryUniCloudCall(
  email: any,
  attachment: Attachment | null,
  reportRows: DmarcRecordRow[],
  cloudFunctionUrl: string
): Promise<void> {
  console.log('🔄 Attempting retry with simplified payload...')
  console.log('📊 Retry attempt details:')
  console.log('  - Email from:', email.from?.address || 'unknown')
  console.log('  - Email subject:', email.subject || 'No subject')
  console.log('  - Has attachment:', !!attachment)
  console.log('  - DMARC records count:', reportRows.length)

  // 重试时使用简化的payload，减少失败的可能性
  const simplifiedPayload = {
    emailInfo: {
      from: email.from?.address || 'unknown',
      to: email.to?.map((addr: Address) => addr?.address || 'unknown').filter((addr: string) => addr !== 'unknown') || ['unknown'],
      subject: email.subject || 'No subject',
      date: email.date || new Date().toISOString(),
      messageId: email.messageId || 'unknown'
    },
    attachment: attachment ? {
      filename: attachment.filename || 'unnamed',
      mimeType: attachment.mimeType || 'application/octet-stream',
      size: typeof attachment.content === 'string' ? attachment.content.length :
        (attachment.content instanceof ArrayBuffer ? attachment.content.byteLength : 0)
    } : null,
    dmarcRecords: reportRows,
    processedAt: new Date().toISOString(),
    workerInfo: {
      version: '1.0.0',
      source: 'cloudflare-workers',
      isRetry: true
    }
  }

  console.log('📦 Simplified payload prepared for retry')
  console.log('📊 Retry payload summary:')
  console.log('  - Payload size:', JSON.stringify(simplifiedPayload).length, 'characters')
  console.log('  - Is retry attempt: true')

  try {
    console.log('📡 Making retry request...')
    const response = await fetch(cloudFunctionUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'User-Agent': 'Cloudflare-Workers-DMARC-Processor/1.0-Retry',
        'X-Is-Retry': 'true',
        'X-Retry-Timestamp': new Date().toISOString()
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
    const result = await response.json()
    console.log('📄 Retry response data:', JSON.stringify(result, null, 2))
  } catch (retryError) {
    console.error('❌ Retry request failed:', retryError)
    throw retryError
  }
}
