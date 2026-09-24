import {test,expect} from '@playwright/test';
test('read, search, progress, reload, next chapter and safe code rendering',async({page})=>{
 const errors=[];page.on('pageerror',e=>errors.push(e.message));
 await page.goto('/');await expect(page.getByRole('heading',{name:'Rust를 배우며, 에이전트를 만들다.'})).toBeVisible();
 await page.screenshot({path:'verification/home-desktop.png',fullPage:true});
 await page.getByRole('link',{name:'첫 페이지 열기'}).click();await expect(page.locator('#article h1')).toContainText('00장');
 await page.getByRole('button',{name:'이 장 학습 완료',exact:true}).click();await expect(page.locator('#progress-text')).toHaveText('1 / 62장 완료');
 await page.reload();await expect(page.getByRole('button',{name:'학습 완료됨 · 취소'})).toBeVisible();
 await page.getByRole('button',{name:'교재 검색'}).click();await page.getByRole('textbox',{name:'검색어'}).fill('얕은 옵션');
 await expect(page.locator('#search-results a')).not.toHaveCount(0);
 await page.getByRole('textbox',{name:'검색어'}).fill('마법없는검색어XYZ123');await expect(page.getByText('검색 결과가 없습니다.')).toBeVisible();
 await page.getByRole('textbox',{name:'검색어'}).fill('숫자를 잃지');await page.locator('#search-results a').first().click();await expect(page.locator('#article h1')).toContainText('38장');
 await expect(page.locator('.code-wrap')).not.toHaveCount(0);await page.screenshot({path:'verification/reader-desktop.png',fullPage:false});
 await page.getByRole('navigation',{name:'장 이동'}).getByText('다음 장 →').click();await expect(page.locator('#article h1')).toContainText('39장');
 expect(errors).toEqual([]);
});
test('interactive labs produce real outcomes and validation errors',async({page})=>{
 await page.goto('/#/playground');await page.getByRole('button',{name:'병합 실행'}).click();await expect(page.locator('#lab-result')).toContainText('"x": 3');await expect(page.locator('#lab-result')).not.toContainText('"y"');
 await page.getByLabel('Run 옵션').fill('null');await page.getByRole('button',{name:'병합 실행'}).click();await expect(page.locator('#lab-result')).toContainText('입력 오류');
 await page.getByRole('tab',{name:'02 생략과 null'}).click();await page.getByRole('button',{name:'인자 복원'}).click();await expect(page.locator('#lab-result')).toHaveText('{}');
 await page.getByRole('button',{name:'명시 null',exact:true}).click();await page.getByRole('button',{name:'인자 복원'}).click();await expect(page.locator('#lab-result')).toContainText('"note": null');
 await page.getByRole('button',{name:'잘못된 입력'}).click();await page.getByRole('button',{name:'인자 복원'}).click();await expect(page.locator('#lab-result')).toContainText('입력 오류');
 await page.getByRole('tab',{name:'03 실행 구간'}).click();await page.getByRole('button',{name:'승인 대기',exact:true}).click();await page.getByRole('button',{name:'취소',exact:true}).click();await expect(page.locator('#state-history')).toContainText('Waiting');await expect(page.locator('#state-history')).toContainText('Cancelled');await expect(page.locator('#segment-id')).toHaveText('02');
 await page.getByRole('button',{name:'재개',exact:true}).click();await expect(page.locator('#state-message')).toContainText('적용할 수 없습니다');
 await page.screenshot({path:'verification/playground-desktop.png',fullPage:true});
 await page.getByRole('tab',{name:'04 Rust 코드'}).click();await page.getByLabel('Rust 2024 · 표준 라이브러리').fill('fn main() { println!("edited"); }');
 const download=page.waitForEvent('download');await page.getByRole('button',{name:'main.rs 내려받기'}).click();expect((await download).suggestedFilename()).toBe('main.rs');
});
test('mobile menu, dark mode, anchors and no horizontal overflow',async({page})=>{
 await page.setViewportSize({width:390,height:844});await page.goto('/');await expect(page.locator('h1')).toBeVisible();
 await page.getByRole('button',{name:'목차 열기'}).click();await expect(page.locator('.sidebar')).toBeInViewport();
 await page.locator('[data-path="ko/00-start.md"]').click();await expect(page.locator('#article h1')).toContainText('00장');
 await page.getByRole('button',{name:'어두운 테마로 변경'}).click();await expect(page.locator('html')).toHaveAttribute('data-theme','dark');
 await page.reload();await expect(page.locator('html')).toHaveAttribute('data-theme','dark');
 expect(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth)).toBe(true);
 await page.screenshot({path:'verification/reader-mobile.png',fullPage:false});
});
test('network failure shows retry instead of empty screen',async({page})=>{
 await page.route('**/catalog.json',route=>route.abort());await page.goto('/');await expect(page.getByRole('button',{name:'다시 시도'})).toBeVisible();
 await page.unroute('**/catalog.json');await page.getByRole('button',{name:'다시 시도'}).click();await expect(page.locator('.hero')).toBeVisible();
});
test('GitHub Pages project path keeps assets and deep-link reloads working',async({page})=>{
 await page.goto('http://127.0.0.1:4183/book/#/read/ko%2F43-provider-contracts.md');
 await expect(page.locator('#article h1')).toContainText('43장');await page.reload();await expect(page.locator('#article h1')).toContainText('43장');
 await page.locator('#section-nav a').first().click();await expect(page).toHaveURL(/section=/);await expect(page.locator('#article h1')).toContainText('43장');
 await page.goto('http://127.0.0.1:4183/book/#/playground');await page.getByRole('button',{name:'병합 실행'}).click();await expect(page.locator('#lab-result')).toContainText('"Run"');
});
test('untrusted markdown cannot execute scripts or inject active links',async({page})=>{
 await page.route('**/docs/*.json',route=>route.fulfill({json:{title:'test',path:'ko/00-start.md',markdown:true,body:'# Sanitized\n\n<script>window.bad=true</script><img src=x onerror="window.bad=true"><a href="javascript:window.bad=true">unsafe</a>\n\n```rust\nfn main() {}\n```'}}));
 await page.goto('/#/read/ko%2F00-start.md');await expect(page.locator('#article h1')).toHaveText('Sanitized');expect(await page.evaluate(()=>window.bad)).toBeUndefined();expect(await page.locator('#article [onerror], #article script, #article a[href^="javascript:"]').count()).toBe(0);
});
