from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from wilberflow.config import NotifyConfig
from wilberflow.notify import build_feishu_workflow_message, send_feishu_text_message, should_send_feishu_notification


class NotifyTests(unittest.TestCase):
    def test_should_send_feishu_notification_respects_status_switches(self) -> None:
        config = NotifyConfig(
            feishu_webhook_url="https://open.feishu.cn/open-apis/bot/v2/hook/demo",
            notify_on_success=False,
            notify_on_failure=True,
        )

        self.assertFalse(should_send_feishu_notification(config, "completed"))
        self.assertTrue(should_send_feishu_notification(config, "failed"))

    def test_build_feishu_workflow_message_uses_chinese_summary(self) -> None:
        text = build_feishu_workflow_message(
            status="failed",
            mode="resume_from_mail",
            batch_id="wf_20260506_120000",
            workspace_root=Path("/tmp/wf_20260506_120000"),
            log_path=Path("/tmp/wf_20260506_120000/logs/pipeline.log"),
            finished_at_utc="2026-05-06T04:00:00Z",
            detail="流程失败: 邮件轮询超时",
        )

        self.assertIn("WilberFlow 运行失败", text)
        self.assertIn("运行模式：补跑收信与下载", text)
        self.assertIn("批次号：wf_20260506_120000", text)
        self.assertIn("说明：流程失败: 邮件轮询超时", text)

    def test_send_feishu_text_message_posts_json_payload(self) -> None:
        config = NotifyConfig(feishu_webhook_url="https://open.feishu.cn/open-apis/bot/v2/hook/demo")

        class DummyResponse:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self) -> bytes:
                return b'{"StatusCode":0}'

        with patch("wilberflow.notify.request.urlopen", return_value=DummyResponse()) as mocked:
            send_feishu_text_message(config, "测试通知")

        request_obj = mocked.call_args.args[0]
        self.assertEqual(request_obj.full_url, config.feishu_webhook_url)
        self.assertEqual(request_obj.get_method(), "POST")
        self.assertIn("测试通知".encode("utf-8"), request_obj.data)


if __name__ == "__main__":
    unittest.main()
