import { Header, type Breadcrumb } from '@/features/layout/Header';

export interface PlaceholderProps {
  title: string;
  breadcrumbs?: Breadcrumb[];
  description?: string;
}

export function Placeholder({ title, breadcrumbs, description }: PlaceholderProps) {
  return (
    <>
      <Header title={title} breadcrumbs={breadcrumbs} description={description} />
      <div className="p-8">
        <div className="rounded-md border border-dashed border-slate-300 bg-white p-10 text-center text-sm text-slate-600">
          Em construção — será implementado em slice dedicada.
        </div>
      </div>
    </>
  );
}
