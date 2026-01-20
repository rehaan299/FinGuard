/**
 * FinGuard Alerts (Google Apps Script)
 *
 * Requires:
 * - Enable BigQuery Advanced Service in Apps Script:
 *   Services → Add a service → BigQuery API
 */

function sendFinGuardAlert() {
  var PROJECT_ID = "YOUR_GCP_PROJECT_ID";
  var DATASET_ID = "finguard";
  var SUMMARY_TABLE = "daily_validation_summary";
  var EXCEPTIONS_TABLE = "transaction_exceptions";

  var RUN_DATE = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd");
  var emailTo = "your_email@example.com";

  // 1) Query summary
  var summaryQuery = `
    SELECT *
    FROM \`${PROJECT_ID}.${DATASET_ID}.${SUMMARY_TABLE}\`
    WHERE run_date = DATE("${RUN_DATE}")
    LIMIT 1
  `;

  var summaryResult = BigQuery.Jobs.query({
    query: summaryQuery,
    useLegacySql: false
  }, PROJECT_ID);

  if (!summaryResult.rows || summaryResult.rows.length === 0) {
    Logger.log("No summary row found for " + RUN_DATE);
    return;
  }

  var row = summaryResult.rows[0].f;
  var total = row[1].v;
  var flagged = row[2].v;
  var high = row[3].v;
  var med = row[4].v;
  var low = row[5].v;
  var flaggedPercent = row[6].v;

  // Only alert if high severity or flagged > 5%
  if (parseInt(high) === 0 && parseFloat(flaggedPercent) < 5.0) {
    Logger.log("No alert needed today.");
    return;
  }

  // 2) Pull top issues
  var issuesQuery = `
    SELECT rule_name, severity, COUNT(*) AS cnt
    FROM \`${PROJECT_ID}.${DATASET_ID}.${EXCEPTIONS_TABLE}\`
    WHERE run_date = DATE("${RUN_DATE}")
    GROUP BY rule_name, severity
    ORDER BY severity DESC, cnt DESC
    LIMIT 8
  `;

  var issuesResult = BigQuery.Jobs.query({
    query: issuesQuery,
    useLegacySql: false
  }, PROJECT_ID);

  var issueLines = [];
  if (issuesResult.rows) {
    issuesResult.rows.forEach(function(r) {
      var rule = r.f[0].v;
      var sev = r.f[1].v;
      var cnt = r.f[2].v;
      issueLines.push(rule + " (" + sev + "): " + cnt);
    });
  }

  var subject = "FinGuard Alert — Exceptions detected for " + RUN_DATE;

  var body =
    "Hi,\n\n" +
    "FinGuard ran validation checks for " + RUN_DATE + " and found exceptions.\n\n" +
    "Summary:\n" +
    "- Total transactions: " + total + "\n" +
    "- Flagged transactions: " + flagged + " (" + flaggedPercent + "%)\n" +
    "- High severity: " + high + "\n" +
    "- Medium severity: " + med + "\n" +
    "- Low severity: " + low + "\n\n" +
    "Top issues:\n" +
    issueLines.join("\n") + "\n\n" +
    "— FinGuard";

  MailApp.sendEmail(emailTo, subject, body);
}
