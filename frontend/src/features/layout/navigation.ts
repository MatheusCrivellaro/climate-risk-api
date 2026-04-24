import {
  BarChart3,
  Droplet,
  ExternalLink,
  FileText,
  LayoutDashboard,
  ListChecks,
  MapPin,
  Package,
  PlayCircle,
  Radar,
  SquareKanban,
  Target,
} from 'lucide-react';
import type { ComponentType, SVGProps } from 'react';

export interface NavItem {
  to: string;
  label: string;
  icon: ComponentType<SVGProps<SVGSVGElement>>;
  end?: boolean;
  external?: boolean;
}

export const NAVIGATION: NavItem[] = [
  { to: '/', label: 'Dashboard', icon: LayoutDashboard, end: true },
  { to: '/execucoes', label: 'Execuções', icon: PlayCircle },
  { to: '/execucoes/nova', label: 'Nova execução', icon: FileText },
  { to: '/execucoes/estresse-hidrico/nova', label: 'Estresse hídrico', icon: Droplet },
  { to: '/jobs', label: 'Jobs', icon: SquareKanban },
  { to: '/calculos/pontos', label: 'Cálculo por pontos', icon: Target },
  { to: '/resultados', label: 'Resultados', icon: BarChart3 },
  { to: '/fornecedores', label: 'Fornecedores', icon: Package },
  { to: '/geocodificacao', label: 'Geocodificação', icon: MapPin },
  { to: '/cobertura', label: 'Cobertura', icon: Radar },
  { to: '/admin', label: 'Admin', icon: ListChecks },
];

export const NAVIGATION_EXTERNA: NavItem[] = [
  {
    to: '/estudo/',
    label: 'Modo simplificado',
    icon: ExternalLink,
    external: true,
  },
];
