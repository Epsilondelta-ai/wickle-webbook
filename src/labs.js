export function objectInput(text) {
  const value = JSON.parse(text);
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('JSON 객체를 입력하세요. null과 배열은 객체가 아닙니다.');
  return value;
}
export function mergeOptions(binding, profile, run) {
  const effective = Object.create(null), sources = Object.create(null);
  for (const [source, values] of [['Binding', binding], ['Profile', profile], ['Run', run]]) {
    for (const [key, value] of Object.entries(values)) { effective[key] = value; sources[key] = source; }
  }
  return { effective, sources };
}
export function decodePresence(value) {
  if (!value || Array.isArray(value) || typeof value !== 'object') throw new Error('Presence는 객체여야 합니다.');
  const keys = Object.keys(value);
  if (keys.length !== 2 || !keys.includes('present') || !keys.includes('value') || typeof value.present !== 'boolean') throw new Error('present(boolean)와 value 두 필드가 모두 필요합니다.');
  if (!value.present && value.value !== null) throw new Error('present=false일 때 value는 null이어야 합니다.');
  return value.present ? { note: value.value } : {};
}
export function transition(state, action) {
  const old = structuredClone(state);
  const reject = message => ({...old, message});
  if (action === 'reset') return initialRun();
  if (action === 'cancel' && !['Succeeded','Cancelled'].includes(old.status)) {
    const idle = old.status !== 'Running';
    if (idle) old.segment++;
    old.status='Cancelled'; old.history.push({segment:old.segment,status:'Cancelled'});
  } else if (action === 'stop' && old.status === 'Running') {
    old.status='Interrupted'; old.history.push({segment:old.segment,status:'Interrupted'});
  } else if (action === 'wait' && old.status === 'Running') {
    old.status='Waiting'; old.history.push({segment:old.segment,status:'Waiting'});
  } else if (action === 'resume' && ['Waiting','Interrupted'].includes(old.status)) {
    old.segment++; old.status='Running';
  } else if (action === 'finish' && old.status === 'Running') {
    old.status='Succeeded'; old.history.push({segment:old.segment,status:'Succeeded'});
  } else return reject('이 상태에서는 선택한 전이를 적용할 수 없습니다.');
  return {...old,message:'전이를 적용했습니다. 이전 구간의 결과는 그대로 남습니다.'};
}
export const initialRun = () => ({status:'Running',segment:1,history:[],message:'실행 구간 1에서 시작합니다.'});
export const rustExamples = {
  ownership: { label:'소유권과 빌림', code:'fn describe(request: &str) {\n    println!("request: {request}");\n}\n\nfn main() {\n    let request = String::from("report");\n    describe(&request);\n    println!("still owned: {request}");\n}\n' },
  effect: { label:'효과를 enum으로 표현하기', code:'#[derive(Debug)]\nenum Effect { NotApplied, Applied, Unknown }\n\nfn main() {\n    for effect in [Effect::NotApplied, Effect::Applied, Effect::Unknown] {\n        let decision = match effect {\n            Effect::NotApplied => "미적용 확인: 정책과 예산을 검사",\n            Effect::Applied => "이미 적용: 다시 실행하지 않음",\n            Effect::Unknown => "불확실: 먼저 조회·확인",\n        };\n        println!("{effect:?}: {decision}");\n    }\n}\n' },
};
export function rustPlaygroundUrl(code) {
  const url = new URL('https://play.rust-lang.org/');
  url.search = new URLSearchParams({version:'stable',mode:'debug',edition:'2024',code}).toString();
  return url.href;
}
