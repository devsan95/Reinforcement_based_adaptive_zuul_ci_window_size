/**
 * RL window status overlay for stock Zuul web UI.
 * Attaches outside React's #root so re-renders do not remove it.
 */
(function () {
  'use strict';

  const BANNER_ID = 'zuul-rl-window-banner';
  const STYLE_ID = 'zuul-rl-window-style';

  function tenantFromPath() {
    const match = window.location.pathname.match(/\/t\/([^/]+)/);
    return match ? match[1] : 'example-tenant';
  }

  function ensureStyle() {
    if (document.getElementById(STYLE_ID)) {
      return;
    }
    const style = document.createElement('style');
    style.id = STYLE_ID;
    style.textContent = [
      `#${BANNER_ID} {`,
      '  position: fixed;',
      '  top: 0;',
      '  left: 0;',
      '  right: 0;',
      '  background: #004368;',
      '  color: #fff;',
      '  padding: 10px 20px;',
      '  font-size: 14px;',
      '  font-family: RedHatText, Overpass, sans-serif;',
      '  border-bottom: 2px solid #06c;',
      '  z-index: 10000;',
      '  box-shadow: 0 2px 6px rgba(0,0,0,0.25);',
      '}',
      'body.zuul-rl-banner-active { padding-top: 44px !important; }',
      '.zuul-rl-queue-badge {',
      '  display: inline-block;',
      '  margin-left: 8px;',
      '  padding: 2px 8px;',
      '  background: #f0ab00;',
      '  color: #151515;',
      '  border-radius: 12px;',
      '  font-size: 12px;',
      '  font-weight: 600;',
      '  vertical-align: 0.1em;',
      '}',
    ].join('\n');
    document.head.appendChild(style);
  }

  function ensureBanner() {
    ensureStyle();
    let banner = document.getElementById(BANNER_ID);
    if (banner) {
      return banner;
    }
    banner = document.createElement('div');
    banner.id = BANNER_ID;
    document.body.insertBefore(banner, document.body.firstChild);
    return banner;
  }

  function formatQueue(q) {
    const delta = q.action_delta;
    const deltaStr = delta == null ? '' :
      ` (${delta >= 0 ? '+' : ''}${delta})`;
    return `${q.name}: current ${q.current_window} → RL ${q.recommended_window}${deltaStr}`;
  }

  function render(data) {
    const banner = ensureBanner();
    const parts = (data.pipelines || [])
      .map((pipeline) => {
        const rl = pipeline.rl_window;
        if (!rl || !rl.queues || !rl.queues.length) {
          return null;
        }
        return `${pipeline.name} [${rl.mode}]: ${rl.queues.map(formatQueue).join(' · ')}`;
      })
      .filter(Boolean);

    if (!parts.length) {
      banner.style.display = 'none';
      document.body.classList.remove('zuul-rl-banner-active');
      return;
    }

    banner.style.display = 'block';
    document.body.classList.add('zuul-rl-banner-active');
    banner.innerHTML =
      '<strong>RL recommended window sizes</strong> — ' +
      parts.join(' &nbsp;|&nbsp; ');
  }

  function enhanceQueueBadges(data) {
    const gate = (data.pipelines || []).find((p) => p.name === 'gate');
    if (!gate || !gate.rl_window || !gate.rl_window.queues) {
      return;
    }

    const rlByWindow = {};
    gate.rl_window.queues.forEach((q) => {
      rlByWindow[q.current_window] = q.recommended_window;
    });

    document.querySelectorAll('.pf-c-badge, .pf-v6-c-badge').forEach((badge) => {
      const text = (badge.textContent || '').trim();
      const match = text.match(/^(\d+)\s*\/\s*(\d+)(?:\s*→\s*RL\s*\d+)?$/);
      if (!match) {
        return;
      }
      const window = parseInt(match[2], 10);
      const rl = rlByWindow[window];
      if (rl == null) {
        return;
      }
      badge.textContent = `${match[1]} / ${window} → RL ${rl}`;
    });

    document.querySelectorAll('.zuul-change-queue-name, .zuul-pipeline-link').forEach((el) => {
      const parent = el.closest('.zuul-change-queue, .zuul-pipeline-summary, .pf-c-card');
      if (!parent) {
        return;
      }
      const text = parent.textContent || '';
      if (!text.includes('gate') && !el.classList.contains('zuul-change-queue-name')) {
        return;
      }
      const q = gate.rl_window.queues[0];
      if (!q) {
        return;
      }
      let tag = parent.querySelector('.zuul-rl-queue-badge');
      if (!tag) {
        tag = document.createElement('span');
        tag.className = 'zuul-rl-queue-badge';
        el.appendChild(tag);
      }
      tag.textContent = `RL rec. ${q.recommended_window}`;
    });
  }

  async function poll() {
    if (!window.location.pathname.includes('/status')) {
      const banner = document.getElementById(BANNER_ID);
      if (banner) {
        banner.style.display = 'none';
      }
      document.body.classList.remove('zuul-rl-banner-active');
      return;
    }

    const tenant = tenantFromPath();
    try {
      const response = await fetch(`/api/tenant/${tenant}/status`);
      if (!response.ok) {
        return;
      }
      const data = await response.json();
      render(data);
      setTimeout(() => enhanceQueueBadges(data), 300);
      setTimeout(() => enhanceQueueBadges(data), 1500);
    } catch (err) {
      // Ignore transient network errors.
    }
  }

  setInterval(poll, 10000);
  poll();

  const observer = new MutationObserver(() => {
    if (window.location.pathname.includes('/status')) {
      poll();
    }
  });
  observer.observe(document.getElementById('root') || document.body, {
    childList: true,
    subtree: true,
  });
})();
